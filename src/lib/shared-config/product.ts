/**
 * Product Configuration
 *
 * Single source of truth for product branding.
 * Used in system prompts, UI, and documentation.
 *
 * Override via environment variables:
 *   DVC_PRODUCT_NAME       - Full product name (default: 'Nile')
 *   DVC_PRODUCT_SHORT_NAME - Short name (default: 'DVC')
 */

/** Default product name */
export const DEFAULT_PRODUCT_NAME = 'Nile';

/** Default short product name */
export const DEFAULT_PRODUCT_SHORT_NAME = 'DVC';

/**
 * Get the product name from environment or default
 */
export function getProductName(): string {
  return process.env.DVC_PRODUCT_NAME || DEFAULT_PRODUCT_NAME;
}

/**
 * Get the short product name from environment or default
 */
export function getProductShortName(): string {
  return process.env.DVC_PRODUCT_SHORT_NAME || DEFAULT_PRODUCT_SHORT_NAME;
}
