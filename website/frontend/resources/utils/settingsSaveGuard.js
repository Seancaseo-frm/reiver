export function canSaveSettings(settingsLoaded, loading = false) {
  return settingsLoaded === true && loading === false;
}
