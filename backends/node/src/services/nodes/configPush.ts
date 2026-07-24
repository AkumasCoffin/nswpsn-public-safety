/**
 * Push a node's merged config to its live agent over the WS hub.
 *
 * Central helper so every trigger (staff PATCH, explicit push endpoint, and
 * the hello/version-mismatch flow) builds + sends the exact same payload.
 */
import { log } from '../../lib/log.js';
import { getNode, type NodeRow } from './registry.js';
import { buildConfigPayload, type ConfigPayload } from './configMerge.js';
import { hub } from './hub.js';

export interface PushResult {
  sent: boolean;
  reason?: 'offline' | 'not_found' | 'presets_unavailable';
  configVersion?: string;
}

/**
 * Build the payload for an already-loaded row and send it if the agent is
 * online. Never throws — preset/build failures resolve to sent:false.
 */
export function pushConfigForNode(node: NodeRow): PushResult {
  let payload: ConfigPayload;
  try {
    payload = buildConfigPayload(node);
  } catch (err) {
    log.warn({ err, nodeId: node.id }, 'config push: could not build payload');
    return { sent: false, reason: 'presets_unavailable' };
  }
  if (!hub.isOnline(node.id)) {
    return { sent: false, reason: 'offline', configVersion: payload.configVersion };
  }
  const ok = hub.sendToAgent(node.id, 'configPush', payload);
  if (ok) {
    log.info(
      { nodeId: node.id, configVersion: payload.configVersion },
      'config pushed to node agent',
    );
  }
  return { sent: ok, reason: ok ? undefined : 'offline', configVersion: payload.configVersion };
}

/** Look the node up by id, then push. */
export async function pushConfigToNode(nodeId: string): Promise<PushResult> {
  const node = await getNode(nodeId);
  if (!node) return { sent: false, reason: 'not_found' };
  return pushConfigForNode(node);
}
