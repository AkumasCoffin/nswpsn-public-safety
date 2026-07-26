/**
 * Push a node's merged config to its live agent over the WS hub.
 *
 * Central helper so every trigger (staff PATCH, explicit push endpoint, and
 * the hello/version-mismatch flow) builds + sends the exact same payload.
 */
import { log } from '../../lib/log.js';
import { getNode, listNodes, type NodeRow } from './registry.js';
import { buildConfigPayload, type ConfigPayload } from './configMerge.js';
import { getGlobalConfig, type GlobalConfig } from './globalConfig.js';
import { hub } from './hub.js';

export interface PushResult {
  sent: boolean;
  reason?: 'offline' | 'not_found' | 'presets_unavailable';
  configVersion?: string;
}

/**
 * Build the payload for an already-loaded row and send it if the agent is
 * online. Never throws — preset/build failures resolve to sent:false. Pass a
 * pre-fetched `global` when fanning out to many nodes to avoid re-reading it.
 */
export async function pushConfigForNode(node: NodeRow, global?: GlobalConfig): Promise<PushResult> {
  let payload: ConfigPayload;
  try {
    payload = await buildConfigPayload(node, global);
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

/**
 * Fan the (freshly-changed) global config out to every online node. Used after
 * a global-config edit so the whole fleet re-syncs. Returns how many were
 * actually pushed. Reads the global config once and reuses it for all nodes.
 */
export async function pushConfigToAllNodes(): Promise<{ pushed: number; total: number }> {
  const [nodes, global] = await Promise.all([listNodes(), getGlobalConfig()]);
  let pushed = 0;
  for (const node of nodes) {
    // Disabled nodes stay connected now (enabled = capture, not connection), so
    // they still receive config — the payload's captureEnabled tells them to stay
    // stopped. Only skip offline nodes.
    if (!hub.isOnline(node.id)) continue;
    const r = await pushConfigForNode(node, global);
    if (r.sent) pushed += 1;
  }
  log.info({ pushed, total: nodes.length }, 'global config fanned out to nodes');
  return { pushed, total: nodes.length };
}
