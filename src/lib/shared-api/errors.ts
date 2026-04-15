/**
 * Shared API Error Types
 *
 * Used by both cloud (api/) and local (api-local/) handlers
 * to ensure consistent error codes and categories.
 */

/** Machine-readable error codes for API responses */
export enum ApiErrorCode {
  // Validation errors
  MISSING_REQUEST_BODY = 'MISSING_REQUEST_BODY',
  MISSING_QUERY_EXECUTION_ID = 'MISSING_QUERY_EXECUTION_ID',
  MISSING_TABLE_NAME = 'MISSING_TABLE_NAME',
  MISSING_CODE = 'MISSING_CODE',
  MISSING_CODE_CONTENT = 'MISSING_CODE_CONTENT',
  VALIDATION_ERROR = 'VALIDATION_ERROR',
  TABLE_VALIDATION_FAILED = 'TABLE_VALIDATION_FAILED',
  SCHEMA_EXTRACTION_FAILED = 'SCHEMA_EXTRACTION_FAILED',

  // Conflict errors
  TABLE_ALREADY_EXISTS = 'TABLE_ALREADY_EXISTS',

  // Not found errors
  QUERY_NOT_FOUND = 'QUERY_NOT_FOUND',

  // Auth errors
  AUTHORIZATION_FAILED = 'AUTHORIZATION_FAILED',

  // Infrastructure errors
  INTERNAL_ERROR = 'INTERNAL_ERROR',
  WORKFLOW_START_FAILED = 'WORKFLOW_START_FAILED',
}

/** Error category for classification */
export type ApiErrorCategory =
  | 'VALIDATION'
  | 'NOT_FOUND'
  | 'ALREADY_EXISTS'
  | 'AUTHORIZATION'
  | 'SERVICE_FAILURE'
  | 'CONFIGURATION';

/**
 * Structured error for API responses.
 * Infrastructure-agnostic -- both Lambda and Express handlers
 * convert this to their transport format.
 */
export interface ApiError {
  message: string;
  errorCode: ApiErrorCode;
  category: ApiErrorCategory;
  field?: string;
  retryable: boolean;
  suggestion?: string;
  context?: Record<string, unknown>;
}

/** Map error codes to HTTP status codes */
export function httpStatusForError(error: ApiError): number {
  switch (error.category) {
    case 'VALIDATION': return 400;
    case 'NOT_FOUND': return 404;
    case 'ALREADY_EXISTS': return 409;
    case 'AUTHORIZATION': return 403;
    case 'SERVICE_FAILURE': return 500;
    case 'CONFIGURATION': return 500;
  }
}
