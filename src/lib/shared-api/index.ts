/**
 * Shared API
 *
 * Shared business logic for API handlers.
 * Pure functions only -- zero infrastructure dependencies.
 */

export { ApiErrorCode, httpStatusForError } from './errors.js';
export type { ApiError, ApiErrorCategory } from './errors.js';

export { validateSATRequest, validateCreateTableFields, tableAlreadyExistsError } from './validation.js';

export {
  filterJobsWithSchedules,
  mapJobsToDefinitions,
  buildCreateTableFromSAT,
} from './transforms.js';
export type { JobDefinitionOutput, BuildCreateTableParams } from './transforms.js';

export {
  normalizeAndFilterDeps,
  parseAndNormalizeDeps,
  isLikelyRealTable,
  mergeActualDeps,
  removeStaleActualDeps,
  buildDepExtractionCode,
  parseDepExtractionResult,
} from './dependency-extraction.js';
export type { ActualDependency, RawDependencyData } from './dependency-extraction.js';

// Generated validators (available after `npm run typespec:generate`)
// Re-exported generated validators
export type { ValidationResult, FieldValidationError } from './generated/validators.js';
export {
  validatePaginationInput,
  validateTableMetadata,
  validateIcebergMetadata,
  validateActualDependency,
  validateCreateTableRequest,
  validateCreateTableResponse,
  validateTableCatalogEntry,
  validateImportTableRequest,
  validateListActivityEventsRequest,
  validateGetTableHistoryRequest,
  validateRollbackToMinorVersionRequest,
  validateCompareVersionsRequest,
} from './generated/validators.js';
