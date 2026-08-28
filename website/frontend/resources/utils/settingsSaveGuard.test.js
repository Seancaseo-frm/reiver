import { describe, expect, it } from 'vitest';
import { canSaveSettings } from './settingsSaveGuard.js';

describe('settings save guard', () => {
  it('blocks saving defaults after a failed initial fetch', () => {
    expect(canSaveSettings(false, false)).toBe(false);
  });

  it('allows saving only after a successful fetch has completed', () => {
    expect(canSaveSettings(true, true)).toBe(false);
    expect(canSaveSettings(true, false)).toBe(true);
  });
});
