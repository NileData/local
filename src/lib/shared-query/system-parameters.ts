/**
 * System Parameter Service
 *
 * Handles system variables (@ScheduleDate, @ScheduleTime) resolution.
 * Resolves built-in date/time variables in query text.
 */

const SYSTEM_VARIABLES = new Set(['ScheduleDate', 'ScheduleTime']);

export interface ExecutionContext {
  scheduledTime?: string | Date;
}

export function isSystemVariable(variableName: string): boolean {
  return SYSTEM_VARIABLES.has(variableName);
}

function parseScheduledTime(scheduledTime: string | Date | undefined): Date {
  if (!scheduledTime) {
    return new Date();
  }
  if (scheduledTime instanceof Date) {
    return scheduledTime;
  }
  if (typeof scheduledTime === 'string') {
    try {
      const isoString = scheduledTime.replace('Z', '+00:00');
      const date = new Date(isoString);
      if (!isNaN(date.getTime())) {
        return date;
      }
    } catch {
      // Fall through to default
    }
  }
  return new Date();
}

/**
 * Resolve system variables (@ScheduleDate, @ScheduleTime) in code.
 * For ad-hoc execution, uses current time.
 *
 * @example
 * resolveSystemVariables(
 *   "SELECT * FROM table WHERE date = '@ScheduleDate'",
 *   { scheduledTime: '2024-01-15T10:30:00Z' }
 * )
 * // Returns: "SELECT * FROM table WHERE date = '2024-01-15'"
 */
export function resolveSystemVariables(
  code: string,
  executionContext: ExecutionContext,
): string {
  if (!code) {
    return code;
  }

  const scheduledTime = parseScheduledTime(executionContext.scheduledTime);

  const scheduleDate = scheduledTime.toISOString().split('T')[0];
  const hours = String(scheduledTime.getUTCHours()).padStart(2, '0');
  const minutes = String(scheduledTime.getUTCMinutes()).padStart(2, '0');
  const seconds = String(scheduledTime.getUTCSeconds()).padStart(2, '0');
  const scheduleTime = `${hours}:${minutes}:${seconds}`;

  let result = code;
  result = result.replace(/@ScheduleDate\b/g, scheduleDate);
  result = result.replace(/@ScheduleTime\b/g, scheduleTime);
  return result;
}

/**
 * Resolve @ScheduleDate based on execution context.
 */
export function resolveScheduleDate(executionContext: ExecutionContext): string {
  const scheduledTime = parseScheduledTime(executionContext.scheduledTime);
  return scheduledTime.toISOString().split('T')[0];
}

/**
 * Resolve a single system variable to its value.
 */
export function resolveSystemVariableValue(
  variableName: string,
  executionContext: ExecutionContext,
): string | null {
  if (!isSystemVariable(variableName)) {
    return null;
  }

  const scheduledTime = parseScheduledTime(executionContext.scheduledTime);

  if (variableName === 'ScheduleDate') {
    return scheduledTime.toISOString().split('T')[0];
  } else if (variableName === 'ScheduleTime') {
    const hours = String(scheduledTime.getUTCHours()).padStart(2, '0');
    const minutes = String(scheduledTime.getUTCMinutes()).padStart(2, '0');
    const seconds = String(scheduledTime.getUTCSeconds()).padStart(2, '0');
    return `${hours}:${minutes}:${seconds}`;
  }

  return null;
}
