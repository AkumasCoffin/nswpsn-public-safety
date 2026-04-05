#!/usr/bin/env python3
"""
NSW PSN Alert Discord Bot
Provides real-time alerts for emergency services, BOM warnings, traffic incidents, and pager messages.
"""

import os
import asyncio
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

import discord
from discord import app_commands
from discord.ext import commands, tasks

from database import Database
from alert_poller import AlertPoller
from embeds import EmbedBuilder

# Load environment variables
load_dotenv()

# Configure logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('nswpsn-bot')

# Set discord.py logging to WARNING to reduce noise (unless DEBUG)
if LOG_LEVEL != 'DEBUG':
    logging.getLogger('discord').setLevel(logging.WARNING)
    logging.getLogger('discord.http').setLevel(logging.WARNING)

# Bot configuration
DISCORD_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
API_BASE_URL = os.getenv('API_BASE_URL', 'http://localhost:8000')
API_KEY = os.getenv('NSWPSN_API_KEY', '')
BOT_OWNER_ID = os.getenv('BOT_OWNER_ID', '')  # Discord User ID for admin commands


def is_bot_owner(user_id: int) -> bool:
    """Check if a user is the bot owner"""
    if not BOT_OWNER_ID:
        return False
    try:
        return int(BOT_OWNER_ID) == user_id
    except ValueError:
        return False

# Alert types available
ALERT_TYPES = {
    'rfs': 'RFS Incidents (Bush Fires)',
    'bom': 'BOM Weather Warnings',
    'traffic_incidents': 'Traffic Incidents',
    'traffic_roadwork': 'Traffic Roadwork',
    'traffic_flood': 'Traffic Flood Hazards',
    'traffic_fire': 'Traffic Fire Hazards',
    'traffic_major': 'Traffic Major Events',
    'power_endeavour': 'Endeavour Power Outages',
    'power_ausgrid': 'Ausgrid Power Outages',
    'waze_hazards': 'Waze Road Hazards',
    'waze_police': 'Waze Police Reports',
    'waze_roadwork': 'Waze Roadwork/Closures',
    'user_incidents': 'User Submitted Incidents',
}

WEBSITE_URL = "https://nswpsn.forcequit.xyz/"


class NSWPSNBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        
        super().__init__(
            command_prefix='!',
            intents=intents,
            help_command=None
        )
        
        self.db = Database()
        self.poller = AlertPoller(API_BASE_URL, API_KEY, self.db)
        self.embed_builder = EmbedBuilder()
        
        # Rate limiting for Discord API
        self.message_queue = asyncio.Queue(maxsize=500)
        self.rate_limit_delay = 0.5  # 500ms between messages
        self.max_messages_per_batch = 10  # Max messages to send per poll cycle
        
        # Track startup time - don't remove guild configs within first 60 seconds
        # This prevents race conditions during startup where guilds may briefly appear disconnected
        self._startup_time = datetime.now()
        self._min_uptime_for_guild_remove = 60  # seconds
        
        # Track permission errors to suppress repeated warnings (debounce)
        self._permission_error_channels: Dict[int, datetime] = {}
        self._permission_error_debounce_seconds = 300  # 5 minutes
        
    async def setup_hook(self):
        """Called when the bot is starting up"""
        logger.info("Setting up bot...")
        logger.info(f"Database path: {self.db.db_path}")
        self.db.init_db()
        
        # Start background tasks
        self.poll_alerts.start()
        self.poll_pager.start()
        self.process_message_queue.start()
        
        # Sync commands globally
        synced = await self.tree.sync()
        logger.info(f"Synced {len(synced)} global commands!")
    
    async def on_ready(self):
        logger.info(f'Logged in as {self.user} (ID: {self.user.id})')
        logger.info(f'Connected to {len(self.guilds)} guilds')
        
        # Log subscription counts with guild details
        logger.info("=== Subscription Summary ===")
        all_guild_ids = set()
        for alert_type in ALERT_TYPES:
            configs = self.db.get_configs_for_alert_type(alert_type)
            if configs:
                logger.info(f"  {alert_type}: {len(configs)} channels")
                for cfg in configs:
                    all_guild_ids.add(cfg['guild_id'])
        pager_configs = self.db.get_pager_configs()
        if pager_configs:
            logger.info(f"  pager: {len(pager_configs)} channels")
            for cfg in pager_configs:
                all_guild_ids.add(cfg['guild_id'])
        
        # Show which guilds have configs vs which we're connected to
        logger.info(f"Guilds with configs in DB: {len(all_guild_ids)}")
        for gid in all_guild_ids:
            guild = self.get_guild(gid)
            if guild:
                logger.info(f"  ✅ {guild.name} ({gid})")
            else:
                logger.warning(f"  ❌ Unknown guild ({gid}) - not connected!")
        
        connected_ids = {g.id for g in self.guilds}
        missing = connected_ids - all_guild_ids
        if missing:
            logger.info(f"Guilds connected but NO configs: {missing}")
            logger.info(f"  (Use /setup in those servers to configure alerts)")
        logger.info(f"⛔ Guild removal BLOCKED for next {self._min_uptime_for_guild_remove}s (startup protection)")
        logger.info("============================")
        
        # Note: Commands are synced globally in setup_hook()
        # Guild-specific sync removed to prevent duplicate commands
        
        # Set bot status
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="NSW Emergency Alerts"
            )
        )
    
    async def on_guild_remove(self, guild: discord.Guild):
        """Called when the bot is removed from a guild - but we DON'T auto-delete configs anymore.
        
        Auto-deletion was causing config loss due to Discord's unreliable guild_remove events
        during network issues/reconnects. Configs should be manually cleaned up using /dev-cleanup.
        """
        # Log for awareness but DO NOT delete anything automatically
        logger.warning(f"⚠️ on_guild_remove triggered for: {guild.name} (ID: {guild.id})")
        logger.warning(f"  → ⛔ AUTO-DELETION DISABLED - configs preserved")
        logger.warning(f"  → Use /dev-cleanup to manually remove stale guild configs if needed")
    
    @tasks.loop(seconds=60)
    async def poll_alerts(self):
        """Poll for new alerts every 60 seconds"""
        try:
            new_alerts = await self.poller.check_alerts()
            
            if new_alerts:
                logger.info(f"📢 Found {len(new_alerts)} new alerts")
                for alert in new_alerts:
                    logger.debug(f"  → {alert['type']}: {alert['id']}")
                    await self.send_alert(alert)
            else:
                logger.debug("No new alerts")
                
        except Exception as e:
            logger.error(f"Error polling alerts: {e}", exc_info=True)
    
    @tasks.loop(seconds=30)
    async def poll_pager(self):
        """Poll for new pager messages every 30 seconds"""
        try:
            new_messages = await self.poller.check_pager()
            
            if new_messages:
                logger.info(f"📟 Found {len(new_messages)} new pager messages")
                for msg in new_messages:
                    logger.debug(f"  → {msg.get('capcode', 'UNKNOWN')}: {msg.get('type', 'Unknown')}")
                    await self.send_pager_message(msg)
            else:
                logger.debug("No new pager messages")
                
        except Exception as e:
            logger.error(f"Error polling pager: {e}", exc_info=True)
    
    @poll_alerts.before_loop
    async def before_poll_alerts(self):
        await self.wait_until_ready()
    
    @poll_pager.before_loop
    async def before_poll_pager(self):
        await self.wait_until_ready()
    
    @tasks.loop(seconds=1)
    async def process_message_queue(self):
        """Process queued messages with rate limiting"""
        messages_sent = 0
        
        while not self.message_queue.empty() and messages_sent < self.max_messages_per_batch:
            try:
                item = self.message_queue.get_nowait()
                channel_id = item['channel_id']
                embed = item['embed']
                content = item.get('content')
                config_id = item.get('config_id')
                config_type = item.get('config_type', 'alert')
                
                try:
                    channel = self.get_channel(channel_id)
                    if not channel:
                        channel = await self.fetch_channel(channel_id)
                    
                    if channel:
                        message = await channel.send(content=content, embed=embed)
                        messages_sent += 1
                        
                        # Debug logging (alerts are already marked as seen in poller)
                        alert_type = item.get('alert_type')
                        if alert_type:
                            logger.debug(f"✅ Sent {alert_type} to #{channel.name}")
                        
                        # Save message URL for incident tracking (RFS alerts)
                        incident_guid = item.get('incident_guid')
                        if incident_guid and message:
                            self.db.save_incident_message(
                                incident_guid=incident_guid,
                                channel_id=channel_id,
                                message_url=message.jump_url,
                                status=item.get('incident_status')
                            )
                        
                        # Rate limit delay
                        if not self.message_queue.empty():
                            await asyncio.sleep(self.rate_limit_delay)
                            
                except discord.NotFound:
                    logger.warning(f"Channel {channel_id} not found, removing config")
                    if config_id:
                        if config_type == 'pager':
                            self.db.remove_pager_config(config_id)
                        else:
                            self.db.remove_config(config_id)
                except discord.Forbidden:
                    # Debounce permission error logging to reduce spam
                    now = datetime.now()
                    last_error = self._permission_error_channels.get(channel_id)
                    if last_error is None or (now - last_error).total_seconds() > self._permission_error_debounce_seconds:
                        logger.warning(f"No permission to send to channel {channel_id}")
                        self._permission_error_channels[channel_id] = now
                except discord.HTTPException as e:
                    if e.status == 429:  # Rate limited
                        logger.warning(f"Rate limited, re-queuing message")
                        await self.message_queue.put(item)  # Re-queue
                        await asyncio.sleep(5)  # Wait 5 seconds
                        break
                    else:
                        logger.error(f"HTTP error sending message: {e}")
                        
            except asyncio.QueueEmpty:
                break
            except Exception as e:
                logger.error(f"Error processing message queue: {e}")
        
        if messages_sent > 0:
            logger.info(f"Sent {messages_sent} messages (queue size: {self.message_queue.qsize()})")
    
    @process_message_queue.before_loop
    async def before_process_queue(self):
        await self.wait_until_ready()
    
    def queue_message(self, channel_id: int, embed: discord.Embed, content: str = None,
                      config_id: int = None, config_type: str = 'alert',
                      incident_guid: str = None, incident_status: str = None,
                      alert_type: str = None, alert_id: str = None):
        """Add a message to the queue for rate-limited sending"""
        item = {
            'channel_id': channel_id,
            'embed': embed,
            'content': content,
            'config_id': config_id,
            'config_type': config_type,
            'incident_guid': incident_guid,
            'incident_status': incident_status,
            'alert_type': alert_type,
            'alert_id': alert_id
        }
        if self.message_queue.full():
            logger.warning(f"Message queue full ({self.message_queue.maxsize}), dropping oldest message")
            try:
                self.message_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        self.message_queue.put_nowait(item)
    
    async def send_alert(self, alert: dict):
        """Queue an alert to all subscribed channels"""
        alert_type = alert.get('type')
        alert_data = alert.get('data', {})
        
        configs = self.db.get_configs_for_alert_type(alert_type)
        
        # Extract incident guid for RFS and user incidents (for tracking/linking)
        incident_guid = None
        incident_status = None
        if alert_type == 'rfs':
            props = alert_data.get('properties', {})
            incident_guid = props.get('guid') or props.get('link') or props.get('title')
            incident_status = props.get('status')
        elif alert_type == 'user_incidents':
            # User incidents use their Supabase ID
            incident_guid = f"user_{alert_data.get('id', '')}"
            incident_status = alert_data.get('status', 'Active')
        
        for config in configs:
            channel_id = config['channel_id']
            
            # For RFS and user incidents, check for previous messages to link
            previous_message = None
            if incident_guid:
                previous_message = self.db.get_first_incident_message(incident_guid, channel_id)
            
            # Build embed with previous message info if available
            embed = self.embed_builder.build_alert_embed(alert, previous_message=previous_message)
            
            content = None
            if config.get('role_id'):
                content = f"<@&{config['role_id']}>"
            
            self.queue_message(
                channel_id=channel_id,
                embed=embed,
                content=content,
                config_id=config['id'],
                config_type='alert',
                incident_guid=incident_guid,
                incident_status=incident_status,
                alert_type=alert_type,
                alert_id=alert.get('id')
            )
    
    async def send_pager_message(self, msg: dict):
        """Queue a pager message to subscribed channels"""
        # Normalize capcode to uppercase string for comparison
        # (API may return int or mixed case, DB stores uppercase strings)
        capcode = str(msg.get('capcode', '')).strip().upper()
        
        # Get all pager configs
        configs = self.db.get_pager_configs()
        
        embed = self.embed_builder.build_pager_embed(msg)
        
        for config in configs:
            # Check if this config should receive this message
            capcodes = config.get('capcodes')  # None means all messages
            
            if capcodes:
                # Normalize stored capcodes to uppercase for comparison
                normalized_capcodes = [str(c).strip().upper() for c in capcodes]
                if capcode not in normalized_capcodes:
                    continue  # Skip if specific capcodes are set and this isn't one of them
            
            content = None
            if config.get('role_id'):
                content = f"<@&{config['role_id']}>"
            
            self.queue_message(
                channel_id=config['channel_id'],
                embed=embed,
                content=content,
                config_id=config['id'],
                config_type='pager',
                alert_type='pager',
                alert_id=msg.get('_msg_hash')  # Use msg_hash for marking as seen
            )


# Create bot instance
bot = NSWPSNBot()


# ==================== SLASH COMMANDS ====================

@bot.tree.command(name="alert", description="Set up alerts for a channel")
@app_commands.describe(
    channel="The channel to send alerts to",
    alert_type="The type of alert to receive (leave empty for ALL alerts)",
    role="Optional role to ping when alerts are sent"
)
@app_commands.choices(alert_type=[
    app_commands.Choice(name=name, value=key) 
    for key, name in ALERT_TYPES.items()
])
@app_commands.default_permissions(manage_channels=True)
async def alert_command(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    alert_type: str = None,
    role: discord.Role = None
):
    """Set up an alert subscription for a channel"""
    try:
        # If no alert type specified, subscribe to ALL
        if alert_type is None:
            # Subscribe to all alert types
            added = []
            updated = []
            
            for atype in ALERT_TYPES.keys():
                existing = bot.db.get_config(interaction.guild_id, channel.id, atype)
                
                if existing:
                    bot.db.update_config(existing['id'], role_id=role.id if role else None)
                    updated.append(ALERT_TYPES[atype])
                else:
                    bot.db.add_config(
                        guild_id=interaction.guild_id,
                        channel_id=channel.id,
                        alert_type=atype,
                        role_id=role.id if role else None
                    )
                    added.append(ALERT_TYPES[atype])
            
            embed = discord.Embed(
                title="✅ All Alerts Configured",
                description=f"Now sending **ALL alert types** to {channel.mention}",
                color=0x00ff00
            )
            embed.add_field(
                name="Alert Types",
                value=f"{len(ALERT_TYPES)} types configured",
                inline=True
            )
            if role:
                embed.add_field(name="Ping Role", value=role.mention, inline=True)
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        
        # Single alert type
        existing = bot.db.get_config(interaction.guild_id, channel.id, alert_type)
        
        if existing:
            # Update existing config
            bot.db.update_config(
                existing['id'],
                role_id=role.id if role else None
            )
            
            embed = discord.Embed(
                title="✅ Alert Updated",
                description=f"Updated **{ALERT_TYPES[alert_type]}** alerts for {channel.mention}",
                color=0x00ff00
            )
            if role:
                embed.add_field(name="Ping Role", value=role.mention, inline=True)
            else:
                embed.add_field(name="Ping Role", value="None", inline=True)
        else:
            # Create new config
            bot.db.add_config(
                guild_id=interaction.guild_id,
                channel_id=channel.id,
                alert_type=alert_type,
                role_id=role.id if role else None
            )
            
            embed = discord.Embed(
                title="✅ Alert Configured",
                description=f"Now sending **{ALERT_TYPES[alert_type]}** alerts to {channel.mention}",
                color=0x00ff00
            )
            if role:
                embed.add_field(name="Ping Role", value=role.mention, inline=True)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        logger.error(f"Error in alert command: {e}")
        await interaction.response.send_message(
            "❌ An error occurred while setting up the alert.",
            ephemeral=True
        )


@bot.tree.command(name="alert-remove", description="Remove alert subscriptions from a channel")
@app_commands.describe(
    channel="The channel to remove alerts from",
    alert_type="The type of alert to remove (leave empty to remove ALL)"
)
@app_commands.choices(alert_type=[
    app_commands.Choice(name=name, value=key) 
    for key, name in ALERT_TYPES.items()
])
@app_commands.default_permissions(manage_channels=True)
async def alert_remove_command(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    alert_type: str = None
):
    """Remove an alert subscription"""
    try:
        # If no alert type specified, remove ALL
        if alert_type is None:
            removed = 0
            for atype in ALERT_TYPES.keys():
                existing = bot.db.get_config(interaction.guild_id, channel.id, atype)
                if existing:
                    bot.db.remove_config(existing['id'])
                    removed += 1
            
            if removed > 0:
                embed = discord.Embed(
                    title="✅ All Alerts Removed",
                    description=f"Removed **{removed} alert types** from {channel.mention}",
                    color=0xff6600
                )
            else:
                embed = discord.Embed(
                    title="❌ Not Found",
                    description=f"No alerts configured for {channel.mention}",
                    color=0xff0000
                )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        
        # Single alert type
        existing = bot.db.get_config(interaction.guild_id, channel.id, alert_type)
        
        if existing:
            bot.db.remove_config(existing['id'])
            
            embed = discord.Embed(
                title="✅ Alert Removed",
                description=f"Removed **{ALERT_TYPES[alert_type]}** alerts from {channel.mention}",
                color=0xff6600
            )
        else:
            embed = discord.Embed(
                title="❌ Not Found",
                description=f"No **{ALERT_TYPES[alert_type]}** alert configured for {channel.mention}",
                color=0xff0000
            )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        logger.error(f"Error in alert-remove command: {e}")
        await interaction.response.send_message(
            "❌ An error occurred while removing the alert.",
            ephemeral=True
        )


@bot.tree.command(name="alert-list", description="List all alert subscriptions for this server")
@app_commands.default_permissions(manage_channels=True)
async def alert_list_command(interaction: discord.Interaction):
    """List all alert subscriptions for the server"""
    try:
        configs = bot.db.get_guild_configs(interaction.guild_id)
        pager_configs = bot.db.get_guild_pager_configs(interaction.guild_id)
        
        embed = discord.Embed(
            title="📋 Alert Subscriptions",
            description=f"Alert configurations for **{interaction.guild.name}**",
            color=0x3498db
        )
        
        if not configs and not pager_configs:
            embed.add_field(
                name="No Subscriptions",
                value="Use `/alert` to set up alerts or `/pager` for pager messages.",
                inline=False
            )
        else:
            # Group by channel
            channels = {}
            for config in configs:
                ch_id = config['channel_id']
                if ch_id not in channels:
                    channels[ch_id] = []
                channels[ch_id].append(config)
            
            for ch_id, ch_configs in channels.items():
                channel = bot.get_channel(ch_id)
                ch_name = channel.mention if channel else f"<#{ch_id}>"
                
                alerts = []
                for cfg in ch_configs:
                    alert_name = ALERT_TYPES.get(cfg['alert_type'], cfg['alert_type'])
                    role_text = f" (pings <@&{cfg['role_id']}>)" if cfg.get('role_id') else ""
                    alerts.append(f"• {alert_name}{role_text}")
                
                embed.add_field(
                    name=f"📢 {ch_name}",
                    value="\n".join(alerts),
                    inline=False
                )
            
            # Add pager configs
            if pager_configs:
                pager_text = []
                for cfg in pager_configs:
                    channel = bot.get_channel(cfg['channel_id'])
                    ch_name = channel.mention if channel else f"<#{cfg['channel_id']}>"
                    
                    if cfg.get('capcodes'):
                        capcodes = cfg['capcodes'].split(',')
                        capcode_text = f" (capcodes: {', '.join(capcodes[:3])}{'...' if len(capcodes) > 3 else ''})"
                    else:
                        capcode_text = " (all messages)"
                    
                    role_text = f" pings <@&{cfg['role_id']}>" if cfg.get('role_id') else ""
                    pager_text.append(f"• {ch_name}{capcode_text}{role_text}")
                
                embed.add_field(
                    name="📟 Pager Subscriptions",
                    value="\n".join(pager_text),
                    inline=False
                )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        logger.error(f"Error in alert-list command: {e}")
        await interaction.response.send_message(
            "❌ An error occurred while listing alerts.",
            ephemeral=True
        )


@bot.tree.command(name="pager", description="Set up pager message alerts for a channel")
@app_commands.describe(
    channel="The channel to send pager messages to",
    capcodes="Comma-separated list of capcodes to filter (leave empty for all messages)",
    role="Optional role to ping when messages are received"
)
@app_commands.default_permissions(manage_channels=True)
async def pager_command(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    capcodes: str = None,
    role: discord.Role = None
):
    """Set up pager message alerts for a channel"""
    try:
        # Normalize capcodes
        capcode_list = None
        if capcodes:
            capcode_list = ','.join([c.strip().upper() for c in capcodes.split(',') if c.strip()])
        
        # Check if already exists
        existing = bot.db.get_pager_config(interaction.guild_id, channel.id)
        
        if existing:
            # Update existing
            bot.db.update_pager_config(
                existing['id'],
                capcodes=capcode_list,
                role_id=role.id if role else None
            )
            
            embed = discord.Embed(
                title="✅ Pager Config Updated",
                description=f"Updated pager alerts for {channel.mention}",
                color=0x00ff00
            )
        else:
            # Create new
            bot.db.add_pager_config(
                guild_id=interaction.guild_id,
                channel_id=channel.id,
                capcodes=capcode_list,
                role_id=role.id if role else None
            )
            
            embed = discord.Embed(
                title="✅ Pager Configured",
                description=f"Now sending pager messages to {channel.mention}",
                color=0x00ff00
            )
        
        if capcode_list:
            codes = capcode_list.split(',')
            embed.add_field(
                name="Capcodes",
                value=f"`{', '.join(codes[:10])}`" + (f" and {len(codes)-10} more" if len(codes) > 10 else ""),
                inline=True
            )
        else:
            embed.add_field(name="Filter", value="All messages", inline=True)
        
        if role:
            embed.add_field(name="Ping Role", value=role.mention, inline=True)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        logger.error(f"Error in pager command: {e}")
        await interaction.response.send_message(
            "❌ An error occurred while setting up pager alerts.",
            ephemeral=True
        )


@bot.tree.command(name="pager-remove", description="Remove pager message alerts from a channel")
@app_commands.describe(
    channel="The channel to remove pager alerts from"
)
@app_commands.default_permissions(manage_channels=True)
async def pager_remove_command(
    interaction: discord.Interaction,
    channel: discord.TextChannel
):
    """Remove pager message alerts"""
    try:
        existing = bot.db.get_pager_config(interaction.guild_id, channel.id)
        
        if existing:
            bot.db.remove_pager_config(existing['id'])
            
            embed = discord.Embed(
                title="✅ Pager Removed",
                description=f"Removed pager alerts from {channel.mention}",
                color=0xff6600
            )
        else:
            embed = discord.Embed(
                title="❌ Not Found",
                description=f"No pager alerts configured for {channel.mention}",
                color=0xff0000
            )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        logger.error(f"Error in pager-remove command: {e}")
        await interaction.response.send_message(
            "❌ An error occurred while removing pager alerts.",
            ephemeral=True
        )


@bot.tree.command(name="status", description="Check the bot's status and connection")
async def status_command(interaction: discord.Interaction):
    """Show bot status"""
    embed = discord.Embed(
        title="🤖 NSW PSN Alert Bot Status",
        color=0x3498db
    )
    
    embed.add_field(
        name="Status",
        value="🟢 Online",
        inline=True
    )
    embed.add_field(
        name="Latency",
        value=f"{round(bot.latency * 1000)}ms",
        inline=True
    )
    embed.add_field(
        name="Servers",
        value=str(len(bot.guilds)),
        inline=True
    )
    
    # Get stats
    total_alerts = bot.db.count_configs()
    total_pager = bot.db.count_pager_configs()
    
    embed.add_field(
        name="Alert Subscriptions",
        value=str(total_alerts),
        inline=True
    )
    embed.add_field(
        name="Pager Subscriptions",
        value=str(total_pager),
        inline=True
    )
    
    embed.add_field(
        name="🌐 Website",
        value=f"[nswpsn.forcequit.xyz]({WEBSITE_URL})",
        inline=False
    )
    
    embed.set_footer(text=f"NSW PSN • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    await interaction.response.send_message(embed=embed)


def _remove_all_alert_configs_for_channel(guild_id: int, channel_id: int) -> int:
    """Remove all alert subscriptions for a given guild+channel."""
    removed = 0
    for cfg in bot.db.get_guild_configs(guild_id):
        if cfg.get("channel_id") == channel_id:
            bot.db.remove_config(cfg["id"])
            removed += 1
    return removed


class PagerCapcodesModal(discord.ui.Modal, title="Pager Filter (Optional)"):
    capcodes = discord.ui.TextInput(
        label="Capcodes (comma-separated, blank = all)",
        required=False,
        max_length=500,
        placeholder="e.g. 1160008,1160056,1440136"
    )

    def __init__(self, on_submit_cb):
        super().__init__()
        self._on_submit_cb = on_submit_cb

    async def on_submit(self, interaction: discord.Interaction):
        raw = (self.capcodes.value or "").strip()
        # Normalize: remove spaces around commas
        normalized = ",".join([p.strip() for p in raw.split(",") if p.strip()]) or None
        await self._on_submit_cb(interaction, normalized)


def _get_alert_configs_for_channel(guild_id: int, channel_id: int) -> List[Dict[str, Any]]:
    return [cfg for cfg in bot.db.get_guild_configs(guild_id) if cfg.get("channel_id") == channel_id]


def _format_alert_configs_for_channel(guild_id: int, channel_id: int) -> str:
    cfgs = _get_alert_configs_for_channel(guild_id, channel_id)
    if not cfgs:
        return "**Off**"

    parts: List[str] = []
    for cfg in sorted(cfgs, key=lambda c: c.get("alert_type", "")):
        atype = cfg.get("alert_type", "")
        name = ALERT_TYPES.get(atype, atype)
        role_id = cfg.get("role_id")
        role_txt = f" (<@&{role_id}>)" if role_id else ""
        parts.append(f"- `{atype}`: {name}{role_txt}")
    return "\n".join(parts)[:1000]


def _format_pager_config_for_channel(guild_id: int, channel_id: int) -> str:
    cfg = bot.db.get_pager_config(guild_id, channel_id)
    if not cfg:
        return "**Off**"

    capcodes = cfg.get("capcodes")
    if isinstance(capcodes, list):
        capcodes_txt = ", ".join(capcodes)
    elif isinstance(capcodes, str) and capcodes.strip():
        capcodes_txt = capcodes.strip()
    else:
        capcodes_txt = "All capcodes"

    role_id = cfg.get("role_id")
    role_txt = f"\nPing Role: <@&{role_id}>" if role_id else ""
    return f"Capcodes: {capcodes_txt}{role_txt}"[:1000]


def _build_setup_home_embed(channel: discord.TextChannel, guild_id: int) -> discord.Embed:
    embed = discord.Embed(
        title="⚙️ Setup",
        description=f"Channel: {channel.mention}\n\nChoose what you want to edit:",
        color=0x3498db
    )
    embed.add_field(name="📢 Alerts", value=_format_alert_configs_for_channel(guild_id, channel.id), inline=False)
    embed.add_field(name="📟 Pager", value=_format_pager_config_for_channel(guild_id, channel.id), inline=False)
    embed.add_field(name="🌐 Website", value=f"[nswpsn.forcequit.xyz]({WEBSITE_URL})", inline=False)
    return embed


async def _edit_or_send(interaction: discord.Interaction, *, embed: discord.Embed, view: Optional[discord.ui.View]):
    try:
        await interaction.response.edit_message(embed=embed, view=view)
    except Exception:
        try:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        except Exception:
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class SetupHomeView(discord.ui.View):
    def __init__(self, invoker_id: int, channel: discord.TextChannel):
        super().__init__(timeout=180)
        self.invoker_id = invoker_id
        self.channel = channel

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker_id

    @discord.ui.button(label="Setup Alerts", style=discord.ButtonStyle.primary)
    async def setup_alerts(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="Setup Alerts",
            description=f"Editing alert subscriptions for {self.channel.mention}\n\n*Select alert types from the dropdown, then click off the dropdown and press **Save**.*",
            color=0x3498db
        )
        embed.add_field(name="Current", value=_format_alert_configs_for_channel(interaction.guild_id, self.channel.id), inline=False)
        embed.add_field(name="🌐 Website", value=f"[nswpsn.forcequit.xyz]({WEBSITE_URL})", inline=False)
        await _edit_or_send(interaction, embed=embed, view=SetupAlertsSubmenuView(self.invoker_id, self.channel))

    @discord.ui.button(label="Setup Pager", style=discord.ButtonStyle.primary)
    async def setup_pager(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="Setup Pager",
            description=f"Editing pager hits for {self.channel.mention}",
            color=0x3498db
        )
        embed.add_field(name="Current", value=_format_pager_config_for_channel(interaction.guild_id, self.channel.id), inline=False)
        embed.add_field(name="🌐 Website", value=f"[nswpsn.forcequit.xyz]({WEBSITE_URL})", inline=False)
        await _edit_or_send(interaction, embed=embed, view=SetupPagerSubmenuView(self.invoker_id, self.channel))


class SetupAlertsSubmenuView(discord.ui.View):
    def __init__(self, invoker_id: int, channel: discord.TextChannel):
        super().__init__(timeout=180)
        self.invoker_id = invoker_id
        self.channel = channel

        existing_cfgs = _get_alert_configs_for_channel(channel.guild.id, channel.id)
        existing_types = sorted([c.get("alert_type") for c in existing_cfgs if c.get("alert_type")])
        self.selected_alert_types: List[str] = existing_types[:]

        self.alert_select.options = [
            discord.SelectOption(label=name, value=key, default=(key in set(existing_types)))
            for key, name in ALERT_TYPES.items()
        ]

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker_id

    @discord.ui.select(
        placeholder="Select alert types to enable",
        min_values=0,
        max_values=len(ALERT_TYPES)
    )
    async def alert_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        self.selected_alert_types = list(select.values)
        await interaction.response.defer(ephemeral=True)

    @discord.ui.button(label="Save", style=discord.ButtonStyle.green)
    async def save(self, interaction: discord.Interaction, button: discord.ui.Button):
        existing_cfgs = _get_alert_configs_for_channel(interaction.guild_id, self.channel.id)
        existing_by_type = {c.get("alert_type"): c for c in existing_cfgs}
        selected = set(self.selected_alert_types)

        for cfg in existing_cfgs:
            atype = cfg.get("alert_type")
            if atype not in selected:
                bot.db.remove_config(cfg["id"])

        for atype in selected:
            if atype in existing_by_type:
                continue
            bot.db.add_config(
                guild_id=interaction.guild_id,
                channel_id=self.channel.id,
                alert_type=atype,
                role_id=None
            )

        home_embed = _build_setup_home_embed(self.channel, interaction.guild_id)
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

    @discord.ui.button(label="Enable All Alerts", style=discord.ButtonStyle.green)
    async def enable_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Enable all alert types for this channel
        for atype in ALERT_TYPES.keys():
            existing = bot.db.get_config(interaction.guild_id, self.channel.id, atype)
            if not existing:
                bot.db.add_config(
                    guild_id=interaction.guild_id,
                    channel_id=self.channel.id,
                    alert_type=atype,
                    role_id=None
                )
        home_embed = _build_setup_home_embed(self.channel, interaction.guild_id)
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

    @discord.ui.button(label="Turn Alerts Off", style=discord.ButtonStyle.danger)
    async def turn_off(self, interaction: discord.Interaction, button: discord.ui.Button):
        _remove_all_alert_configs_for_channel(interaction.guild_id, self.channel.id)
        home_embed = _build_setup_home_embed(self.channel, interaction.guild_id)
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        home_embed = _build_setup_home_embed(self.channel, interaction.guild_id)
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))


class SetupPagerSubmenuView(discord.ui.View):
    def __init__(self, invoker_id: int, channel: discord.TextChannel):
        super().__init__(timeout=180)
        self.invoker_id = invoker_id
        self.channel = channel

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker_id

    @discord.ui.button(label="Turn Pager Off", style=discord.ButtonStyle.danger)
    async def turn_off(self, interaction: discord.Interaction, button: discord.ui.Button):
        cfg = bot.db.get_pager_config(interaction.guild_id, self.channel.id)
        if cfg:
            bot.db.remove_pager_config(cfg["id"])
        home_embed = _build_setup_home_embed(self.channel, interaction.guild_id)
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

    @discord.ui.button(label="All Pager Hits", style=discord.ButtonStyle.green)
    async def all_hits(self, interaction: discord.Interaction, button: discord.ui.Button):
        existing = bot.db.get_pager_config(interaction.guild_id, self.channel.id)
        role_id = existing.get("role_id") if existing else None
        bot.db.add_pager_config(
            guild_id=interaction.guild_id,
            channel_id=self.channel.id,
            capcodes=None,
            role_id=role_id
        )
        home_embed = _build_setup_home_embed(self.channel, interaction.guild_id)
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

    @discord.ui.button(label="Filter Capcodes…", style=discord.ButtonStyle.primary)
    async def filter_capcodes(self, interaction: discord.Interaction, button: discord.ui.Button):
        async def _after_modal(modal_interaction: discord.Interaction, capcodes: Optional[str]):
            existing = bot.db.get_pager_config(modal_interaction.guild_id, self.channel.id)
            role_id = existing.get("role_id") if existing else None
            bot.db.add_pager_config(
                guild_id=modal_interaction.guild_id,
                channel_id=self.channel.id,
                capcodes=capcodes,
                role_id=role_id
            )
            home_embed = _build_setup_home_embed(self.channel, modal_interaction.guild_id)
            await _edit_or_send(modal_interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

        await interaction.response.send_modal(PagerCapcodesModal(_after_modal))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        home_embed = _build_setup_home_embed(self.channel, interaction.guild_id)
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))


@bot.tree.command(name="setup", description="Interactive setup wizard for alerts and/or pager hits")
@app_commands.describe(
    channel="Channel to configure (defaults to current channel)"
)
@app_commands.default_permissions(manage_channels=True)
async def setup_command(
    interaction: discord.Interaction,
    channel: Optional[discord.TextChannel] = None
):
    # NOTE: /setup must not change configs by default. It only shows current config and allows editing.
    if channel is None:
        if isinstance(interaction.channel, discord.TextChannel):
            channel = interaction.channel
        else:
            await interaction.response.send_message("❌ Please specify a channel for `/setup`.", ephemeral=True)
            return

    embed = _build_setup_home_embed(channel, interaction.guild_id)
    await interaction.response.send_message(embed=embed, view=SetupHomeView(interaction.user.id, channel), ephemeral=True)


@bot.tree.command(name="help", description="Show available commands and how to use the bot")
async def help_command(interaction: discord.Interaction):
    """Show help information"""
    embed = discord.Embed(
        title="📚 NSW PSN Alert Bot - Help",
        description="Get real-time alerts for NSW emergencies, traffic, weather warnings, and pager messages.",
        color=0x3498db
    )
    
    # Alert Commands
    embed.add_field(
        name="📢 Alert Commands",
        value=(
            "`/setup` - Interactive setup wizard\n"
            "`/alert` - Set up alerts for a channel\n"
            "`/alert-remove` - Remove alerts from a channel\n"
            "`/alert-list` - List all alert subscriptions"
        ),
        inline=False
    )
    
    # Pager Commands
    embed.add_field(
        name="📟 Pager Commands",
        value=(
            "`/pager` - Set up pager message alerts\n"
            "`/pager-remove` - Remove pager alerts"
        ),
        inline=False
    )
    
    # Info Commands
    embed.add_field(
        name="ℹ️ Info Commands",
        value=(
            "`/summary` - Dashboard of current incidents\n"
            "`/status` - Check bot status\n"
            "`/help` - Show this help message"
        ),
        inline=False
    )
    
    # Alert Types
    embed.add_field(
        name="🚨 Available Alert Types",
        value=(
            "• `rfs` - Bush Fire Incidents\n"
            "• `bom` - BOM Weather Warnings (all) ⭐\n"
            "• `traffic_incidents` - Traffic Incidents\n"
            "• `traffic_roadwork` - Roadwork\n"
            "• `traffic_flood` - Flood Hazards\n"
            "• `traffic_fire` - Fire Hazards\n"
            "• `traffic_major` - Major Events\n"
            "• `power_endeavour` - Endeavour Energy Outages\n"
            "• `power_ausgrid` - Ausgrid Outages\n"
            "• `user_incidents` - User Submitted Incidents"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🌐 Website",
        value=f"[nswpsn.forcequit.xyz]({WEBSITE_URL})",
        inline=False
    )
    
    embed.set_footer(text="NSW PSN • Use /alert with no alert_type to subscribe to ALL alerts")
    
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="summary", description="Show a summary of current incidents across NSW")
async def summary_command(interaction: discord.Interaction):
    """Show a dashboard of current incidents and stats"""
    await interaction.response.defer()
    
    import aiohttp
    from datetime import datetime, timedelta, timezone
    
    embed = discord.Embed(
        title="📊 NSW Incident Summary",
        description="Current status across all monitored services",
        color=0x3498db,
        timestamp=datetime.now()
    )
    
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'User-Agent': 'NSWPSNBot/1.0',
        'X-Client-Type': 'discord-bot'
    }
    
    # Fetch stats summary from API (already has all the counts)
    stats = None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_BASE_URL}/api/stats/summary", headers=headers, timeout=15) as resp:
                if resp.status == 200:
                    stats = await resp.json()
    except Exception as e:
        logger.error(f"Error fetching stats summary: {e}")
    
    if not stats:
        stats = {'power': {}, 'traffic': {}, 'emergency': {}}
    
    # === POWER OUTAGES ===
    power = stats.get('power', {})
    
    # Endeavour
    endeavour = power.get('endeavour', {})
    endeavour_current = endeavour.get('current', 0)
    endeavour_planned = endeavour.get('future', 0)
    
    endeavour_text = f"⚡ Current: **{endeavour_current}**\n🔧 Planned: **{endeavour_planned}**"
    embed.add_field(name="🔌 Endeavour", value=endeavour_text, inline=True)
    
    # Ausgrid
    ausgrid = power.get('ausgrid', {})
    ausgrid_outages = ausgrid.get('outages', 0)
    ausgrid_customers = ausgrid.get('customersAffected', 0)
    
    ausgrid_text = f"⚡ Outages: **{ausgrid_outages}**"
    if ausgrid_customers > 0:
        ausgrid_text += f"\n👥 Affected: **{ausgrid_customers:,}**"
    embed.add_field(name="🔌 Ausgrid", value=ausgrid_text, inline=True)
    
    # === TRAFFIC INCIDENTS ===
    traffic = stats.get('traffic', {})
    crashes = traffic.get('crashes', 0)
    hazards = traffic.get('hazards', 0)
    breakdowns = traffic.get('breakdowns', 0)
    total_incidents = traffic.get('incidents', 0)
    changed_conditions = total_incidents - crashes - hazards - breakdowns
    
    traffic_text = f"💥 Crashes: **{crashes}**\n⚠️ Hazards: **{hazards}**\n🚗 Breakdowns: **{breakdowns}**"
    if changed_conditions > 0:
        traffic_text += f"\n🚧 Changed Conditions: **{changed_conditions}**"
    embed.add_field(name="🚦 Traffic Incidents", value=traffic_text, inline=True)
    
    # === ROADWORK ===
    roadwork_count = traffic.get('roadwork', 0)
    embed.add_field(name="🚧 Active Roadwork", value=f"**{roadwork_count}** sites", inline=True)
    
    # === ROAD FIRES ===
    fire_count = traffic.get('fires', 0)
    embed.add_field(name="🔥 Road Fire Hazards", value=f"**{fire_count}** active", inline=True)
    
    # === RFS INCIDENTS ===
    emergency = stats.get('emergency', {})
    rfs_count = emergency.get('rfs_incidents', 0)
    embed.add_field(name="🔥 RFS Major Incidents", value=f"**{rfs_count}** active", inline=True)
    
    # === FLOOD HAZARDS ===
    flood_count = traffic.get('floods', 0)
    embed.add_field(name="🌊 Flood Hazards", value=f"**{flood_count}** active", inline=True)
    
    # === BOM WARNINGS ===
    bom = emergency.get('bom_warnings', {})
    land_warnings = bom.get('land', 0)
    marine_warnings = bom.get('marine', 0)
    
    bom_text = f"🌍 Land: **{land_warnings}**\n🌊 Marine: **{marine_warnings}**"
    embed.add_field(name="⛈️ BOM Warnings", value=bom_text, inline=True)
    
    # === PAGER HITS ===
    # Query API for pager counts at different time windows
    pager_counts = {'1h': 0, '6h': 0, '12h': 0, '24h': 0}
    
    try:
        # Fetch pager counts for each time window from API
        for hours_key, hours_val in [('24h', 24), ('12h', 12), ('6h', 6), ('1h', 1)]:
            async with aiohttp.ClientSession() as session:
                url = f"{API_BASE_URL}/api/pager/hits"
                params = {'hours': str(hours_val), 'limit': '2000'}
                async with session.get(url, headers=headers, params=params, timeout=15) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if isinstance(data, dict):
                            pager_counts[hours_key] = data.get('count', 0)
                    else:
                        logger.warning(f"API pager query returned {resp.status} for {hours_key}")
    except Exception as e:
        logger.error(f"Error fetching pager counts: {e}")
    
    pager_text = (
        f"Last 1h: **{pager_counts['1h']}**\n"
        f"Last 6h: **{pager_counts['6h']}**\n"
        f"Last 12h: **{pager_counts['12h']}**\n"
        f"Last 24h: **{pager_counts['24h']}**"
    )
    embed.add_field(name="📟 Pager Hits", value=pager_text, inline=True)
    
    # Footer with website link
    embed.add_field(
        name="🌐 Live Map",
        value=f"[View full dashboard on nswpsn.forcequit.xyz]({WEBSITE_URL})",
        inline=False
    )
    
    embed.set_footer(text="NSW PSN • Data refreshed just now")
    
    await interaction.followup.send(embed=embed)


async def build_dev_status_embed(show_sensitive: bool = True) -> discord.Embed:
    """Build the dev status embed
    
    Args:
        show_sensitive: If True, show API URLs and other sensitive info
    """
    import platform
    import sys
    
    embed = discord.Embed(
        title="🔧 Dev Status - Diagnostics",
        color=0x9b59b6,
        timestamp=datetime.now()
    )
    
    # System Info
    embed.add_field(
        name="💻 System",
        value=f"```\nPython: {sys.version.split()[0]}\nOS: {platform.system()} {platform.release()}\ndiscord.py: {discord.__version__}```",
        inline=False
    )
    
    # Bot Stats
    embed.add_field(name="🟢 Status", value="Online", inline=True)
    embed.add_field(name="📡 Latency", value=f"{round(bot.latency * 1000)}ms", inline=True)
    embed.add_field(name="🏠 Guilds", value=str(len(bot.guilds)), inline=True)
    
    # Task Status
    alerts_status = "🟢 Running" if bot.poll_alerts.is_running() else "🔴 Stopped"
    pager_status = "🟢 Running" if bot.poll_pager.is_running() else "🔴 Stopped"
    queue_status = "🟢 Running" if bot.process_message_queue.is_running() else "🔴 Stopped"
    queue_size = bot.message_queue.qsize()
    
    embed.add_field(
        name="⏱️ Background Tasks",
        value=f"Alert Poller: {alerts_status}\nPager Poller: {pager_status}\nMessage Queue: {queue_status}",
        inline=False
    )
    
    embed.add_field(
        name="📬 Message Queue",
        value=f"Pending: {queue_size}\nRate Limit: {bot.rate_limit_delay}s\nBatch Size: {bot.max_messages_per_batch}",
        inline=True
    )
    
    # Poll intervals
    embed.add_field(
        name="🔄 Poll Intervals",
        value=f"Alerts: 60s\nPager: 30s",
        inline=True
    )
    
    # Database Stats
    total_alerts = bot.db.count_configs()
    total_pager = bot.db.count_pager_configs()
    
    embed.add_field(
        name="📊 Subscriptions",
        value=f"Alert Configs: {total_alerts}\nPager Configs: {total_pager}",
        inline=True
    )
    
    # Configuration
    api_configured = "🟢 Yes" if API_BASE_URL else "🔴 No"
    
    if show_sensitive:
        embed.add_field(
            name="⚙️ Configuration",
            value=f"API URL: `{API_BASE_URL[:30]}...`\nAPI: {api_configured}",
            inline=False
        )
    else:
        embed.add_field(
            name="⚙️ Configuration",
            value=f"API: {api_configured}",
            inline=False
        )
    
    # Guild Details
    guild_list = []
    for guild in bot.guilds[:10]:  # Limit to 10
        member_count = guild.member_count or 0
        guild_list.append(f"• {guild.name} ({member_count} members)")
    
    if guild_list:
        guild_text = "\n".join(guild_list)
        if len(bot.guilds) > 10:
            guild_text += f"\n... and {len(bot.guilds) - 10} more"
        embed.add_field(
            name="🏠 Guilds",
            value=guild_text,
            inline=False
        )
    
    # All alert configs breakdown
    all_configs = []
    for guild in bot.guilds:
        configs = bot.db.get_guild_configs(guild.id)
        pager_configs = bot.db.get_guild_pager_configs(guild.id)
        if configs or pager_configs:
            all_configs.append(f"• {guild.name}: {len(configs)} alerts, {len(pager_configs)} pager")
    
    if all_configs:
        config_text = "\n".join(all_configs[:10])
        embed.add_field(
            name="📋 Config Breakdown",
            value=config_text or "None",
            inline=False
        )
    
    embed.set_footer(text=f"Bot ID: {bot.user.id}")
    
    return embed


@bot.tree.command(name="dev-status", description="[Dev Only] Detailed bot diagnostics (hidden)")
async def dev_status_command(interaction: discord.Interaction):
    """Show detailed bot status - Dev only, hidden"""
    if not is_bot_owner(interaction.user.id):
        await interaction.response.send_message(
            "❌ This command is restricted to the bot developer.",
            ephemeral=True
        )
        return
    
    await interaction.response.defer(ephemeral=True)
    
    embed = await build_dev_status_embed()
    embed.set_footer(text=f"Bot ID: {bot.user.id} | Dev ID: {BOT_OWNER_ID}")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="dev-status-public", description="[Dev Only] Detailed bot diagnostics (visible to channel)")
async def dev_status_public_command(interaction: discord.Interaction):
    """Show detailed bot status - Dev only, visible to everyone"""
    if not is_bot_owner(interaction.user.id):
        await interaction.response.send_message(
            "❌ This command is restricted to the bot developer.",
            ephemeral=True
        )
        return
    
    await interaction.response.defer()
    
    embed = await build_dev_status_embed(show_sensitive=False)
    embed.set_footer(text=f"Bot ID: {bot.user.id}")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="dev-clear-seen", description="[Dev Only] Clear seen message cache")
async def dev_clear_seen_command(interaction: discord.Interaction):
    """Clear the seen messages cache - Dev only"""
    if not is_bot_owner(interaction.user.id):
        await interaction.response.send_message(
            "❌ This command is restricted to the bot developer.",
            ephemeral=True
        )
        return
    
    try:
        bot.db.cleanup_old_seen(days=0)  # Clear all
        await interaction.response.send_message(
            "✅ Cleared seen message cache. New messages will be sent again.",
            ephemeral=True
        )
    except Exception as e:
        await interaction.response.send_message(
            f"❌ Error clearing cache: {e}",
            ephemeral=True
        )


@bot.tree.command(name="dev-channel", description="[Dev Only] Set a dev channel to receive ALL alerts")
@app_commands.describe(
    channel="Channel to send all alerts to (leave empty to disable)",
    role="Optional role to ping"
)
async def dev_channel_command(
    interaction: discord.Interaction,
    channel: discord.TextChannel = None,
    role: discord.Role = None
):
    """Set up a dev channel that receives ALL alerts - Dev only"""
    if not is_bot_owner(interaction.user.id):
        await interaction.response.send_message(
            "❌ This command is restricted to the bot developer.",
            ephemeral=True
        )
        return
    
    if channel is None:
        # Remove dev channel - remove all alert types
        removed = 0
        for atype in ALERT_TYPES.keys():
            existing = bot.db.get_config(interaction.guild_id, 0, atype)  # guild_id 0 = dev channel marker
            if existing:
                bot.db.remove_config(existing['id'])
                removed += 1
        
        # Also check for actual dev channel configs
        configs = bot.db.get_guild_configs(interaction.guild_id)
        for cfg in configs:
            if cfg.get('is_dev_channel'):
                bot.db.remove_config(cfg['id'])
                removed += 1
        
        embed = discord.Embed(
            title="✅ Dev Channel Disabled",
            description="Dev channel alerts have been disabled.",
            color=0xff6600
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return
    
    # Set up dev channel - subscribe to ALL alert types + pager
    added = 0
    for atype in ALERT_TYPES.keys():
        existing = bot.db.get_config(interaction.guild_id, channel.id, atype)
        if existing:
            bot.db.update_config(existing['id'], role_id=role.id if role else None)
        else:
            bot.db.add_config(
                guild_id=interaction.guild_id,
                channel_id=channel.id,
                alert_type=atype,
                role_id=role.id if role else None
            )
        added += 1
    
    # Also add pager (all messages)
    existing_pager = bot.db.get_pager_config(interaction.guild_id, channel.id)
    if existing_pager:
        bot.db.update_pager_config(existing_pager['id'], capcodes=None, role_id=role.id if role else None)
    else:
        bot.db.add_pager_config(
            guild_id=interaction.guild_id,
            channel_id=channel.id,
            capcodes=None,
            role_id=role.id if role else None
        )
    
    embed = discord.Embed(
        title="✅ Dev Channel Configured",
        description=f"Now sending **ALL alerts** to {channel.mention}",
        color=0x00ff00
    )
    embed.add_field(name="Alert Types", value=f"{len(ALERT_TYPES)} types", inline=True)
    embed.add_field(name="Pager", value="All messages", inline=True)
    if role:
        embed.add_field(name="Ping Role", value=role.mention, inline=True)
    
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="dev-test", description="[Dev Only] Send current real alerts to this channel (not live)")
@app_commands.describe(
    alert_type="Type of alert to fetch and send (leave empty for all)"
)
@app_commands.choices(alert_type=[
    app_commands.Choice(name="All Alerts", value="all"),
    app_commands.Choice(name="Pager Messages", value="pager"),
    app_commands.Choice(name="RFS Incidents", value="rfs"),
    app_commands.Choice(name="BOM Warnings", value="bom"),
    app_commands.Choice(name="Traffic Incidents", value="traffic_incidents"),
    app_commands.Choice(name="Traffic Roadwork", value="traffic_roadwork"),
    app_commands.Choice(name="Traffic Flood", value="traffic_flood"),
    app_commands.Choice(name="Traffic Fire", value="traffic_fire"),
    app_commands.Choice(name="Traffic Major Events", value="traffic_major"),
    app_commands.Choice(name="Power - Endeavour", value="power_endeavour"),
    app_commands.Choice(name="Power - Ausgrid", value="power_ausgrid"),
    app_commands.Choice(name="User Incidents", value="user_incidents"),
])
async def dev_test_command(
    interaction: discord.Interaction,
    alert_type: str = "all"
):
    """Fetch and send current real alerts - Dev only"""
    if not is_bot_owner(interaction.user.id):
        await interaction.response.send_message(
            "❌ This command is restricted to the bot developer.",
            ephemeral=True
        )
        return
    
    await interaction.response.defer(ephemeral=True)
    
    import aiohttp
    
    sent = 0
    errors = []
    
    # Determine which types to fetch
    if alert_type == "all":
        types_to_fetch = list(bot.poller.endpoints.keys()) + ["pager", "user_incidents"]
    else:
        types_to_fetch = [alert_type]
    
    for atype in types_to_fetch:
        try:
            if atype == "pager":
                # Fetch pager from API
                messages = await bot.poller._fetch_pager_from_api()
                # Send up to 3 most recent
                for msg in messages[:3]:
                    parsed = bot.poller._format_api_pager(msg)
                    if parsed:
                        embed = bot.embed_builder.build_pager_embed(parsed)
                        await interaction.channel.send(embed=embed)
                        sent += 1
                        await asyncio.sleep(0.5)
            elif atype == "user_incidents":
                # Fetch user incidents from Supabase
                incidents = await bot.poller._fetch_user_incidents()
                # Send up to 3 most recent
                for inc in incidents[:3]:
                    alert = {'type': 'user_incidents', 'data': inc}
                    embed = bot.embed_builder.build_alert_embed(alert)
                    await interaction.channel.send(embed=embed)
                    sent += 1
                    await asyncio.sleep(0.5)
                if not incidents:
                    errors.append(f"{atype}: No active incidents (or Supabase not configured)")
            else:
                # Fetch from API
                endpoint = bot.poller.endpoints.get(atype)
                if not endpoint:
                    continue
                
                data = await bot.poller._fetch(endpoint)
                if not data:
                    errors.append(f"{atype}: No data")
                    continue
                
                items = bot.poller._extract_items(atype, data)
                
                # Send up to 2 per type
                for item in items[:2]:
                    alert = {
                        'type': atype,
                        'data': item
                    }
                    embed = bot.embed_builder.build_alert_embed(alert)
                    await interaction.channel.send(embed=embed)
                    sent += 1
                    await asyncio.sleep(0.5)
                
                if not items:
                    errors.append(f"{atype}: No active alerts")
                    
        except Exception as e:
            errors.append(f"{atype}: {str(e)[:50]}")
            logger.error(f"Dev test error for {atype}: {e}")
    
    # Summary
    summary = f"✅ Sent **{sent}** real alert(s) to this channel."
    if errors:
        summary += f"\n\n⚠️ Issues:\n" + "\n".join(f"• {e}" for e in errors[:5])
    
    await interaction.followup.send(summary, ephemeral=True)


@bot.tree.command(name="dev-sync", description="[Dev Only] Force sync slash commands")
async def dev_sync_command(interaction: discord.Interaction):
    """Force sync slash commands - Dev only"""
    if not is_bot_owner(interaction.user.id):
        await interaction.response.send_message(
            "❌ This command is restricted to the bot developer.",
            ephemeral=True
        )
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Clear guild-specific commands first (to remove duplicates)
        bot.tree.clear_commands(guild=interaction.guild)
        await bot.tree.sync(guild=interaction.guild)
        
        # Sync globally
        global_synced = await bot.tree.sync()
        
        await interaction.followup.send(
            f"✅ Cleared guild commands and synced {len(global_synced)} commands globally.\n"
            f"Duplicates should be gone now!",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(
            f"❌ Error syncing commands: {e}",
            ephemeral=True
        )


@bot.tree.command(name="dev-cleanup", description="[Dev Only] Remove configs for guilds the bot has left")
async def dev_cleanup_command(interaction: discord.Interaction):
    """Remove configs for guilds the bot is no longer in - Dev only"""
    if not is_bot_owner(interaction.user.id):
        await interaction.response.send_message(
            "❌ This command is restricted to the bot developer.",
            ephemeral=True
        )
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Get all guild IDs that have configs
        all_configs = bot.db.get_all_alert_configs()
        pager_configs = bot.db.get_pager_configs()
        
        config_guild_ids = set()
        for cfg in all_configs:
            config_guild_ids.add(cfg['guild_id'])
        for cfg in pager_configs:
            config_guild_ids.add(cfg['guild_id'])
        
        # Get guild IDs the bot is actually in
        connected_guild_ids = {guild.id for guild in bot.guilds}
        
        # Find orphaned guild configs
        orphaned = config_guild_ids - connected_guild_ids
        
        if not orphaned:
            await interaction.followup.send(
                "✅ No orphaned configs found - all configured guilds are connected.",
                ephemeral=True
            )
            return
        
        # Show orphaned guilds and ask for confirmation
        orphan_list = "\n".join([f"• Guild ID: `{gid}`" for gid in orphaned])
        
        # For safety, just list them - require explicit command to remove
        await interaction.followup.send(
            f"⚠️ Found **{len(orphaned)}** guild(s) with configs that the bot is no longer in:\n\n"
            f"{orphan_list}\n\n"
            f"To remove these configs, use:\n"
            f"`/dev-cleanup-confirm {' '.join(str(g) for g in orphaned)}`",
            ephemeral=True
        )
        
    except Exception as e:
        await interaction.followup.send(
            f"❌ Error checking configs: {e}",
            ephemeral=True
        )


@bot.tree.command(name="dev-cleanup-confirm", description="[Dev Only] Confirm removal of orphaned configs")
@app_commands.describe(guild_ids="Space-separated guild IDs to remove configs for")
async def dev_cleanup_confirm_command(interaction: discord.Interaction, guild_ids: str):
    """Actually remove configs for specified guilds - Dev only"""
    if not is_bot_owner(interaction.user.id):
        await interaction.response.send_message(
            "❌ This command is restricted to the bot developer.",
            ephemeral=True
        )
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Parse guild IDs
        ids_to_remove = []
        for gid_str in guild_ids.split():
            try:
                ids_to_remove.append(int(gid_str.strip()))
            except ValueError:
                continue
        
        if not ids_to_remove:
            await interaction.followup.send("❌ No valid guild IDs provided.", ephemeral=True)
            return
        
        # Verify these guilds are actually not connected
        connected_guild_ids = {guild.id for guild in bot.guilds}
        
        results = []
        for gid in ids_to_remove:
            if gid in connected_guild_ids:
                results.append(f"⚠️ `{gid}` - SKIPPED (bot is still in this guild)")
            else:
                removed = bot.db.remove_guild_data(gid)
                results.append(f"✅ `{gid}` - Removed {removed['alerts']} alert configs, {removed['pager']} pager configs")
        
        await interaction.followup.send(
            f"**Cleanup Results:**\n" + "\n".join(results),
            ephemeral=True
        )
        
    except Exception as e:
        await interaction.followup.send(
            f"❌ Error during cleanup: {e}",
            ephemeral=True
        )


@bot.tree.command(name="dev-setup", description="[Dev Only] Setup wizard for any server (bypasses permissions)")
@app_commands.describe(
    guild_id="The guild ID to configure",
    channel_id="The channel ID to configure"
)
async def dev_setup_command(
    interaction: discord.Interaction,
    guild_id: str,
    channel_id: str
):
    """Interactive setup wizard for any server - Dev only (bypasses manage_channels permission)"""
    if not is_bot_owner(interaction.user.id):
        await interaction.response.send_message(
            "❌ This command is restricted to the bot developer.",
            ephemeral=True
        )
        return
    
    try:
        gid = int(guild_id)
        cid = int(channel_id)
        
        # Verify bot is in the guild
        guild = bot.get_guild(gid)
        if not guild:
            await interaction.response.send_message(
                f"❌ Bot is not in guild `{gid}`",
                ephemeral=True
            )
            return
        
        # Verify channel exists
        channel = guild.get_channel(cid)
        if not channel:
            await interaction.response.send_message(
                f"❌ Channel `{cid}` not found in guild `{guild.name}`",
                ephemeral=True
            )
            return
        
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message(
                f"❌ Channel must be a text channel",
                ephemeral=True
            )
            return
        
        # Use the same embed and view as /setup
        embed = _build_setup_home_embed(channel, gid)
        await interaction.response.send_message(
            embed=embed,
            view=SetupHomeView(interaction.user.id, channel),
            ephemeral=True
        )
        
    except ValueError:
        await interaction.response.send_message(
            "❌ Invalid ID format. Guild ID and Channel ID must be numbers.",
            ephemeral=True
        )
    except Exception as e:
        await interaction.response.send_message(
            f"❌ Error: {e}",
            ephemeral=True
        )


# ==================== DEV BROADCAST ====================

class BroadcastConfirmView(discord.ui.View):
    """Confirmation view for broadcast command"""
    def __init__(self, invoker_id: int, message: str, embed: discord.Embed):
        super().__init__(timeout=120)
        self.invoker_id = invoker_id
        self.message = message
        self.embed = embed
        self.cancelled = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker_id

    @discord.ui.button(label="Send to All Servers", style=discord.ButtonStyle.danger, emoji="📢")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Immediately disable buttons and respond to avoid timeout
        for item in self.children:
            item.disabled = True
        
        # Respond immediately with "Sending..." status
        sending_embed = discord.Embed(
            title="📢 Broadcasting...",
            description="Sending message to all servers. Please wait...",
            color=0xf39c12
        )
        await interaction.response.edit_message(embed=sending_embed, view=self)
        
        # Get all unique channels from configs
        all_configs = bot.db.get_all_alert_configs()
        channel_ids = set()
        for cfg in all_configs:
            channel_ids.add(cfg['channel_id'])
        
        # Also get pager channels
        pager_configs = bot.db.get_pager_configs()
        for cfg in pager_configs:
            channel_ids.add(cfg['channel_id'])
        
        success = 0
        failed = 0
        failed_guilds = []
        
        for channel_id in channel_ids:
            try:
                channel = bot.get_channel(channel_id)
                if channel and isinstance(channel, discord.TextChannel):
                    await channel.send(embed=self.embed)
                    success += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                failed_guilds.append(f"{channel_id}: {str(e)[:50]}")
        
        # Update with results
        result_embed = discord.Embed(
            title="📢 Broadcast Complete",
            description=f"✅ Sent to **{success}** channels\n❌ Failed: **{failed}** channels",
            color=0x2ecc71 if failed == 0 else 0xf39c12
        )
        if failed_guilds:
            result_embed.add_field(
                name="Failed Channels",
                value="\n".join(failed_guilds[:10]) + (f"\n...and {len(failed_guilds)-10} more" if len(failed_guilds) > 10 else ""),
                inline=False
            )
        
        await interaction.edit_original_response(embed=result_embed, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.cancelled = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="❌ Broadcast cancelled.", embed=None, view=self)


@bot.tree.command(name="dev-broadcast", description="[Dev Only] Send a message to all configured servers")
@app_commands.describe(
    title="Title of the broadcast message",
    message="The message to broadcast",
    color="Embed color (hex, e.g., FF5733) - default: blue"
)
async def dev_broadcast_command(
    interaction: discord.Interaction,
    title: str,
    message: str,
    color: str = "3498db"
):
    """Send a message to all servers with configured channels - Dev only"""
    if not is_bot_owner(interaction.user.id):
        await interaction.response.send_message(
            "❌ This command is restricted to the bot developer.",
            ephemeral=True
        )
        return
    
    # Parse color
    try:
        embed_color = int(color.replace("#", "").replace("0x", ""), 16)
    except ValueError:
        embed_color = 0x3498db  # Default blue
    
    # Build preview embed
    broadcast_embed = discord.Embed(
        title=f"📢 {title}",
        description=message,
        color=embed_color,
        timestamp=datetime.now()
    )
    broadcast_embed.set_footer(text="NSW PSN Alert Bot • Broadcast Message")
    
    # Count channels
    all_configs = bot.db.get_all_alert_configs()
    pager_configs = bot.db.get_pager_configs()
    
    channel_ids = set()
    for cfg in all_configs:
        channel_ids.add(cfg['channel_id'])
    for cfg in pager_configs:
        channel_ids.add(cfg['channel_id'])
    
    guild_ids = set()
    for cfg in all_configs:
        guild_ids.add(cfg['guild_id'])
    for cfg in pager_configs:
        guild_ids.add(cfg['guild_id'])
    
    # Preview embed
    preview_embed = discord.Embed(
        title="📢 Broadcast Preview",
        description=f"This will send the message below to:\n• **{len(channel_ids)}** channels\n• **{len(guild_ids)}** servers\n\n**Preview:**",
        color=0xf39c12
    )
    
    await interaction.response.send_message(
        embeds=[preview_embed, broadcast_embed],
        view=BroadcastConfirmView(interaction.user.id, message, broadcast_embed),
        ephemeral=True
    )


# ==================== MAIN ====================

def main():
    if not DISCORD_TOKEN:
        logger.error("DISCORD_BOT_TOKEN environment variable not set!")
        logger.error("Please create a .env file with DISCORD_BOT_TOKEN=your_token_here")
        return
    
    logger.info("Starting NSW PSN Alert Bot...")
    bot.run(DISCORD_TOKEN)


if __name__ == '__main__':
    main()

