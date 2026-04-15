/**
 * System Parameter Service
 *
 * Re-exports from shared-query for backwards compatibility.
 */

export {
  isSystemVariable,
  resolveSystemVariables,
  resolveScheduleDate,
  resolveSystemVariableValue,
} from '../lib/shared-query/index.js';

export type { ExecutionContext } from '../lib/shared-query/index.js';
