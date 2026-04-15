/**
 * Parameter Substitution Service
 *
 * Re-exports from shared-query for backwards compatibility.
 */

export {
  detectParameters,
  validateParameterName,
  formatParameterValue,
  validateParametersSecurity,
  substituteParameters,
  getUnusedParameters,
  getMissingParameters,
} from '../lib/shared-query/index.js';
