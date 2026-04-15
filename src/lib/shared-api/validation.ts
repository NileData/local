/**
 * Shared Request Validation
 *
 * Pure validation functions used by both cloud and local API handlers.
 * Returns ApiError on failure, null on success.
 */

import { ApiError, ApiErrorCode } from './errors.js';

/** Minimal shape needed for SAT validation (avoids importing full generated types) */
interface SATRequestFields {
  queryExecutionId?: string;
  tableName?: string;
  code?: { content?: string };
}

/**
 * Validate required fields for SaveQueryAsTableRequest.
 * Validates required fields for SaveQueryAsTableRequest.
 */
export function validateSATRequest(body: SATRequestFields): ApiError | null {
  if (!body.queryExecutionId) {
    return {
      message: 'Query execution ID is required.',
      errorCode: ApiErrorCode.MISSING_QUERY_EXECUTION_ID,
      category: 'VALIDATION',
      field: 'queryExecutionId',
      retryable: false,
      suggestion: 'First run a query using execute_query tool and use the returned queryExecutionId',
    };
  }

  if (!body.tableName) {
    return {
      message: 'Table name is required.',
      errorCode: ApiErrorCode.MISSING_TABLE_NAME,
      category: 'VALIDATION',
      field: 'tableName',
      retryable: false,
      suggestion: 'Provide a valid table name (lowercase letters, numbers, underscores, 3-64 chars)',
    };
  }

  if (!body.code) {
    return {
      message: 'Code definition is required.',
      errorCode: ApiErrorCode.MISSING_CODE,
      category: 'VALIDATION',
      field: 'code',
      retryable: false,
      suggestion: 'Provide a code object with content (SQL/PySpark), language, and dialect',
    };
  }

  return null;
}

/**
 * Validate required fields for CreateTable request.
 */
export function validateCreateTableFields(body: { database?: string; tableName?: string }): ApiError | null {
  if (!body.database || !body.tableName) {
    return {
      message: '"database" and "tableName" are required.',
      errorCode: ApiErrorCode.VALIDATION_ERROR,
      category: 'VALIDATION',
      retryable: false,
    };
  }
  return null;
}

/**
 * Build a TABLE_ALREADY_EXISTS error.
 */
export function tableAlreadyExistsError(database: string, tableName: string): ApiError {
  return {
    message: `Table '${database}.${tableName}' already exists.`,
    errorCode: ApiErrorCode.TABLE_ALREADY_EXISTS,
    category: 'ALREADY_EXISTS',
    field: 'tableName',
    retryable: false,
    suggestion: 'Choose a different table name or use a different database',
  };
}
