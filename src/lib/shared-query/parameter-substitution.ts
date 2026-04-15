/**
 * Parameter Substitution Service
 *
 * Handles query parameter substitution with security validation.
 * Handles @param substitution with type coercion and security checks.
 */

import { ParameterType } from './generated/types.js';
import type { QueryParameter } from './generated/types.js';

// Parameter detection pattern
const PARAMETER_PATTERN = /@([a-zA-Z_][a-zA-Z0-9_]*)/g;

// SQL keywords that cannot be used as parameter names
const SQL_KEYWORDS = new Set([
  'select', 'from', 'where', 'insert', 'update', 'delete', 'drop', 'create',
  'alter', 'table', 'database', 'index', 'view', 'procedure', 'function',
]);

// Dangerous patterns for security validation
const DANGEROUS_PATTERNS = [
  /;.*DROP\s+TABLE/i,
  /;.*DELETE\s+FROM/i,
  /;.*INSERT\s+INTO/i,
  /;.*UPDATE\s+.*SET/i,
  /--/,
  /\/\*.*\*\//,
  /xp_cmdshell/i,
  /UNION.*SELECT/i,
];

/**
 * Detect all @parameter references in code.
 *
 * @param code - SQL or Python query string
 * @returns Array of unique parameter names (without @ prefix)
 */
export function detectParameters(code: string): string[] {
  const matches: string[] = [];
  let match;
  PARAMETER_PATTERN.lastIndex = 0;
  while ((match = PARAMETER_PATTERN.exec(code)) !== null) {
    matches.push(match[1]);
  }
  return [...new Set(matches)];
}

/**
 * Validate parameter name follows naming conventions.
 */
export function validateParameterName(name: string): boolean {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    return false;
  }
  return !SQL_KEYWORDS.has(name.toLowerCase());
}

function escapeSqlValue(value: string): string {
  return value.replace(/'/g, "''");
}

function validateDateFormat(value: string): string {
  const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
  if (!dateRegex.test(value)) {
    throw new Error(`Invalid date format: ${value}. Expected YYYY-MM-DD`);
  }
  const date = new Date(value);
  if (isNaN(date.getTime())) {
    throw new Error(`Invalid date value: ${value}`);
  }
  return value;
}

function validateTimestampFormat(value: string): string {
  const formats = [
    /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/,
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/,
    /^\d{4}-\d{2}-\d{2}$/,
  ];
  const isValid = formats.some(format => format.test(value));
  if (!isValid) {
    throw new Error(`Invalid timestamp format: ${value}`);
  }
  const date = new Date(value);
  if (isNaN(date.getTime())) {
    throw new Error(`Invalid timestamp value: ${value}`);
  }
  return date.toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, '');
}

function validateInteger(value: string): number {
  const num = parseInt(value, 10);
  if (isNaN(num)) {
    throw new Error(`Invalid integer value: ${value}`);
  }
  return num;
}

function formatListParameter(value: string): string {
  const items = value.split(',').map(item => item.trim()).filter(Boolean);
  if (items.length === 0) {
    throw new Error('List parameter cannot be empty');
  }
  return items.map(item => `'${escapeSqlValue(item)}'`).join(',');
}

/**
 * Format parameter value based on type for SQL substitution.
 */
export function formatParameterValue(param: QueryParameter): string {
  const { paramType, value } = param;

  if (!value && paramType !== ParameterType.String) {
    throw new Error(`Parameter '${param.name}' requires a value for type '${paramType}'`);
  }

  switch (paramType) {
    case ParameterType.String:
      return `'${escapeSqlValue(value)}'`;
    case ParameterType.Date:
      return `DATE '${validateDateFormat(value)}'`;
    case ParameterType.Timestamp:
      return `TIMESTAMP '${validateTimestampFormat(value)}'`;
    case ParameterType.Int:
    case ParameterType.Long:
      return String(validateInteger(value));
    case ParameterType.Double: {
      const num = parseFloat(value);
      if (isNaN(num)) {
        throw new Error(`Invalid double value: ${value}`);
      }
      return String(num);
    }
    case ParameterType.Boolean:
      if (value.toLowerCase() !== 'true' && value.toLowerCase() !== 'false') {
        throw new Error(`Invalid boolean value: ${value}`);
      }
      return value.toUpperCase();
    case ParameterType.Array:
      return formatListParameter(value);
    default:
      return `'${escapeSqlValue(value)}'`;
  }
}

/**
 * Security validation for parameter values.
 */
export function validateParametersSecurity(parameters: QueryParameter[]): void {
  for (const param of parameters) {
    const { name, value } = param;
    for (const pattern of DANGEROUS_PATTERNS) {
      if (pattern.test(value)) {
        throw new Error(`Security violation in parameter '${name}': suspicious pattern detected`);
      }
    }
    if (param.paramType === ParameterType.String && value.length > 1000) {
      throw new Error(`Parameter '${name}' value too long (max 1000 characters)`);
    }
  }
}

/**
 * Replace @parameter references in code with formatted values.
 * Case-insensitive matching for parameter names.
 *
 * @example
 * substituteParameters(
 *   "SELECT * FROM table WHERE date = @start_date",
 *   [{ name: 'start_date', paramType: ParameterType.Date, value: '2024-01-01', isUsed: true }]
 * )
 * // Returns: "SELECT * FROM table WHERE date = DATE '2024-01-01'"
 */
export function substituteParameters(code: string, parameters: QueryParameter[]): string {
  validateParametersSecurity(parameters);

  const usedParams = parameters.filter(p => p.isUsed);

  const substitutions = new Map<string, string>();
  for (const param of usedParams) {
    if (!validateParameterName(param.name)) {
      throw new Error(`Invalid parameter name: ${param.name}`);
    }
    substitutions.set(param.name.toLowerCase(), formatParameterValue(param));
  }

  const parameterPattern = /@([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
  return code.replace(parameterPattern, (match, paramName: string) => {
    const lowerParamName = paramName.toLowerCase();
    if (substitutions.has(lowerParamName)) {
      return substitutions.get(lowerParamName)!;
    }
    return match;
  });
}

/**
 * Identify parameters defined but not used in code.
 */
export function getUnusedParameters(code: string, parameters: QueryParameter[]): string[] {
  const detectedLower = new Set(detectParameters(code).map(p => p.toLowerCase()));
  return parameters.map(p => p.name).filter(name => !detectedLower.has(name.toLowerCase()));
}

/**
 * Identify @parameters in code that have no definition.
 */
export function getMissingParameters(code: string, parameters: QueryParameter[]): string[] {
  const detected = detectParameters(code);
  const definedLower = new Set(parameters.filter(p => p.isUsed).map(p => p.name.toLowerCase()));
  return detected.filter(name => !definedLower.has(name.toLowerCase()));
}
