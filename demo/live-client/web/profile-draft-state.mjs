// createDraftModel keeps one generation-aware immutable editor state for a
// profile draft. It never persists anything to browser storage; destroy clears
// retained source and preview bytes.
export function createDraftModel() {
  let draft = null;
  let selectedPath = '';
  let source = '';
  let savedSource = '';
  let conflict = false;
  let preview = null;
  let destroyed = false;

  function guard() {
    if (destroyed) throw new Error('draft model destroyed');
  }

  return Object.freeze({
    loadDraft(next) {
      guard();
      draft = next;
      conflict = false;
      preview = null;
      if (next === null || !next.templates.some((entry) => entry.path === selectedPath)) {
        selectedPath = '';
        source = '';
        savedSource = '';
      }
    },
    // applySaved replaces the generation after a successful save while keeping
    // the current editor selection.
    applySaved(next) {
      guard();
      draft = next;
      conflict = false;
      preview = null;
      savedSource = source;
    },
    markConflict() {
      guard();
      conflict = true;
    },
    clearConflict() {
      guard();
      conflict = false;
    },
    selectTemplate(path, loadedSource) {
      guard();
      selectedPath = path;
      source = loadedSource;
      savedSource = loadedSource;
    },
    editSource(text) {
      guard();
      if (typeof text !== 'string') throw new TypeError('template source must be a string');
      source = text;
    },
    setPreview(next) {
      guard();
      preview = next;
    },
    snapshot() {
      guard();
      return Object.freeze({
        draft,
        selectedPath,
        source,
        dirty: selectedPath !== '' && source !== savedSource,
        conflict,
        preview,
      });
    },
    destroy() {
      destroyed = true;
      draft = null;
      selectedPath = '';
      source = '';
      savedSource = '';
      preview = null;
      conflict = false;
    },
  });
}
