/**
 * Shared Query Types
 *
 * Minimal type definitions used by the parameter substitution module.
 * Extracted from the full TypeSpec-generated types.
 */

export enum ParameterType {
  String = "string",
  Int = "int",
  Long = "long",
  Double = "double",
  Boolean = "boolean",
  Date = "date",
  Timestamp = "timestamp",
  Array = "array",
}

export enum SystemVariable {
  ScheduleDate = "ScheduleDate",
  ScheduleTime = "ScheduleTime",
}

export interface QueryParameter {
  /** Parameter name (without `@` prefix) */
  name: string;
  /** Parameter value (as string, will be cast to appropriate type) */
  value: string;
  paramType: ParameterType;
  /** Whether parameter was detected/used in query */
  isUsed?: boolean;
  [k: string]: unknown;
}
