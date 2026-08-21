import {createDraftModel} from './profile-draft-state.mjs';

export const PROFILE_TABS = Object.freeze(['runtime', 'profile', 'prompts', 'workers', 'adapters', 'knowledge', 'publish']);

const NOTICES = Object.freeze({
  '': '',
  loaded: 'Draft loaded.',
  created: 'Draft created.',
  saved: 'Draft saved.',
  template_saved: 'Template saved.',
  template_deleted: 'Template deleted.',
  recipe_saved: 'Worker recipe saved.',
  recipe_deleted: 'Worker recipe removed.',
  binding_saved: 'Adapter binding saved.',
  binding_deleted: 'Adapter disabled for this draft.',
  previewed_document: 'Document preview ready. Review the excerpts, then publish.',
  published_document: 'Knowledge version published.',
  tombstoned: 'Knowledge version retired from new selection.',
  unsupported_content: 'Only UTF-8 text and Markdown are accepted. Convert other documents first.',
  content_limit: 'The document exceeds a size limit.',
  knowledge_invalid: 'The document could not be turned into citable excerpts.',
  knowledge_not_found: 'That knowledge version no longer exists.',
  knowledge_tombstoned: 'That knowledge version is retired.',
  invalid_pack_id: 'Knowledge pack IDs use letters, digits, dot, hyphen, and underscore.',
  knowledge_pinned: 'Knowledge version pinned to this draft.',
  knowledge_unpinned: 'Knowledge pack unpinned from this draft.',
  validated: 'Draft is valid and publishable.',
  previewed: 'Preview rendered.',
  published: 'Profile version published.',
  activated: 'Version activated for new sessions.',
  revoked: 'Version revoked.',
  stale_draft: 'The draft changed elsewhere. Reload before saving again.',
  profile_invalid: 'The draft is not valid. Check the profile ID and template sources.',
  profile_incompatible: 'The draft is incompatible with the deployed engine or adapters.',
  publication_conflict: 'This draft generation was already published.',
  profile_revoked: 'That version is revoked.',
  store_unavailable: 'The profile store is unavailable.',
  network_unavailable: 'Unable to contact the local setup service.',
  invalid_response: 'The setup service returned an invalid response.',
  invalid_template_path: 'Template paths must be lowercase logical .j2 names.',
  invalid_recipe_id: 'Worker recipe IDs use lowercase letters, digits, hyphen, and underscore.',
  objective_template_missing: 'Add the objective template for this worker before saving the recipe.',
  recipe_invalid: 'Worker recipe limits are out of range.',
  admin_disabled: 'Vertical profiles are not enabled on this demo.',
  busy: 'Another profile action is still running.',
});

export function profileNoticeText(code) {
  return NOTICES[code] ?? 'The profile action could not be completed.';
}

// createProfileWorkspace orchestrates the Profile, Prompts, and Review &
// publish tabs against the strict profile API. One action runs at a time;
// destroy aborts any in-flight request and clears retained source bytes.
export function createProfileWorkspace({api, dom}) {
  if (!api || !dom || typeof dom.render !== 'function' || typeof dom.announce !== 'function') {
    throw new TypeError('profile workspace requires api and DOM ports');
  }
  const model = createDraftModel();
  let tab = 'runtime';
  let profileID = '';
  let published = Object.freeze([]);
  let catalog = Object.freeze({registryVersion: '', adapters: Object.freeze([])});
  let notice = '';
  let adminAvailable = true;
  let busy = false;
  let controller = null;
  let destroyed = false;

  function snapshot() {
    const editor = model.snapshot();
    return Object.freeze({
      tab,
      busy,
      adminAvailable,
      profileID,
      draft: editor.draft,
      selectedPath: editor.selectedPath,
      source: editor.source,
      dirty: editor.dirty,
      conflict: editor.conflict,
      preview: editor.preview,
      recipes: editor.draft === null ? Object.freeze([]) : editor.draft.recipes,
      knowledge: editor.draft === null ? Object.freeze([]) : editor.draft.knowledge,
      bindings: editor.draft === null ? Object.freeze([]) : (editor.draft.bindings ?? Object.freeze([])),
      catalog,
      published,
      notice,
      publishDisabled: editor.draft === null || editor.dirty || editor.conflict,
    });
  }

  function render() {
    if (destroyed) return;
    dom.render(snapshot());
  }

  function announce(code) {
    notice = code;
    dom.announce(profileNoticeText(code));
    render();
  }

  async function act(run, successCode) {
    if (destroyed) return false;
    if (busy) {
      announce('busy');
      return false;
    }
    busy = true;
    controller = new AbortController();
    const signal = controller.signal;
    render();
    try {
      await run(signal);
      if (!destroyed && !signal.aborted) announce(successCode);
      return true;
    } catch (error) {
      if (destroyed || signal.aborted) return false;
      const code = typeof error?.code === 'string' ? error.code : 'network_unavailable';
      if (code === 'stale_draft') model.markConflict();
      announce(code === 'http_404' ? 'admin_disabled' : code);
      return false;
    } finally {
      busy = false;
      controller = null;
      render();
    }
  }

  return Object.freeze({
    snapshot,
    selectTab(next) {
      if (!PROFILE_TABS.includes(next)) return;
      tab = next;
      render();
    },
    // openOrCreate loads an existing draft or creates a new one seeded with
    // the default system template.
    openOrCreate(id, description) {
      return act(async (signal) => {
        let draft = await api.loadDraft(id, signal);
        if (draft === null) {
          draft = await api.createProfile(id, description ?? '', signal);
          notice = 'created';
        }
        profileID = draft.id;
        model.loadDraft(draft);
        catalog = await api.adapterCatalog(signal);
      }, 'loaded');
    },
    saveDescription(description) {
      return act(async (signal) => {
        const current = model.snapshot().draft;
        if (!current) throw Object.assign(new Error('profile_invalid'), {code: 'profile_invalid'});
        const draft = await api.saveDescription(profileID, current.generation, description, signal);
        model.loadDraft(draft);
      }, 'saved');
    },
    selectTemplate(path) {
      return act(async (signal) => {
        const sourceText = await api.templateSource(profileID, path, signal);
        model.selectTemplate(path, sourceText);
      }, '');
    },
    editSource(text) {
      model.editSource(text);
      render();
    },
    saveTemplate(path, sourceText) {
      return act(async (signal) => {
        const current = model.snapshot().draft;
        if (!current) throw Object.assign(new Error('profile_invalid'), {code: 'profile_invalid'});
        const draft = await api.saveTemplate(profileID, current.generation, path, sourceText, signal);
        model.applySaved(draft);
        model.selectTemplate(path, sourceText);
      }, 'template_saved');
    },
    deleteTemplate(path) {
      return act(async (signal) => {
        const current = model.snapshot().draft;
        if (!current) throw Object.assign(new Error('profile_invalid'), {code: 'profile_invalid'});
        await api.deleteTemplate(profileID, current.generation, path, signal);
        const draft = await api.loadDraft(profileID, signal);
        model.loadDraft(draft);
      }, 'template_deleted');
    },
    // saveRecipe writes one worker recipe. The objective template must already
    // exist in the bundle; the service relinks it, so no template reference
    // ever crosses the browser boundary.
    saveRecipe(recipeID, settings) {
      return act(async (signal) => {
        const current = model.snapshot().draft;
        if (!current) throw Object.assign(new Error('profile_invalid'), {code: 'profile_invalid'});
        const draft = await api.saveRecipe(profileID, current.generation, recipeID, settings, signal);
        model.applySaved(draft);
      }, 'recipe_saved');
    },
    deleteRecipe(recipeID) {
      return act(async (signal) => {
        const current = model.snapshot().draft;
        if (!current) throw Object.assign(new Error('profile_invalid'), {code: 'profile_invalid'});
        await api.deleteRecipe(profileID, current.generation, recipeID, signal);
        const draft = await api.loadDraft(profileID, signal);
        model.loadDraft(draft);
      }, 'recipe_deleted');
    },
    saveBinding(adapterID, binding) {
      return act(async (signal) => {
        const current = model.snapshot().draft;
        if (!current) throw Object.assign(new Error('profile_invalid'), {code: 'profile_invalid'});
        model.applySaved(await api.saveBinding(profileID, current.generation, adapterID, binding, signal));
      }, 'binding_saved');
    },
    deleteBinding(adapterID) {
      return act(async (signal) => {
        const current = model.snapshot().draft;
        if (!current) throw Object.assign(new Error('profile_invalid'), {code: 'profile_invalid'});
        model.applySaved(await api.deleteBinding(profileID, current.generation, adapterID, signal));
      }, 'binding_deleted');
    },
    // pinKnowledge attaches a published version to the draft. The generation
    // advances, so a stale editor cannot pin against a draft that moved.
    pinKnowledge(packID, version) {
      return act(async (signal) => {
        const current = model.snapshot().draft;
        if (!current) throw Object.assign(new Error('profile_invalid'), {code: 'profile_invalid'});
        model.applySaved(await api.pinKnowledge(profileID, current.generation, packID, version, signal));
      }, 'knowledge_pinned');
    },
    unpinKnowledge(packID) {
      return act(async (signal) => {
        const current = model.snapshot().draft;
        if (!current) throw Object.assign(new Error('profile_invalid'), {code: 'profile_invalid'});
        await api.unpinKnowledge(profileID, current.generation, packID, signal);
        model.loadDraft(await api.loadDraft(profileID, signal));
      }, 'knowledge_unpinned');
    },
    reloadDraft() {
      return act(async (signal) => {
        const draft = await api.loadDraft(profileID, signal);
        if (draft === null) throw Object.assign(new Error('invalid_response'), {code: 'invalid_response'});
        model.loadDraft(draft);
      }, 'loaded');
    },
    validateDraft() {
      return act(async (signal) => {
        const current = model.snapshot().draft;
        if (!current) throw Object.assign(new Error('profile_invalid'), {code: 'profile_invalid'});
        await api.validate(profileID, current.generation, signal);
      }, 'validated');
    },
    previewDraft() {
      return act(async (signal) => {
        const current = model.snapshot().draft;
        if (!current) throw Object.assign(new Error('profile_invalid'), {code: 'profile_invalid'});
        model.setPreview(await api.preview(profileID, current.generation, signal));
      }, 'previewed');
    },
    publishDraft(activate) {
      return act(async (signal) => {
        const editor = model.snapshot();
        if (!editor.draft || editor.dirty || editor.conflict) {
          throw Object.assign(new Error('profile_invalid'), {code: 'profile_invalid'});
        }
        await api.publish(profileID, editor.draft.generation, activate, signal);
        published = await api.listPublished(signal);
      }, 'published');
    },
    refreshPublished() {
      return act(async (signal) => {
        published = await api.listPublished(signal);
      }, '');
    },
    activateVersion(id, version) {
      return act(async (signal) => {
        await api.activate(id, version, signal);
        published = await api.listPublished(signal);
      }, 'activated');
    },
    revokeVersion(id, version) {
      return act(async (signal) => {
        await api.revoke(id, version, signal);
        published = await api.listPublished(signal);
      }, 'revoked');
    },
    markAdminUnavailable() {
      adminAvailable = false;
      announce('admin_disabled');
    },
    destroy() {
      destroyed = true;
      if (controller !== null) controller.abort();
      controller = null;
      model.destroy();
      published = Object.freeze([]);
      catalog = Object.freeze({registryVersion: '', adapters: Object.freeze([])});
      profileID = '';
    },
  });
}
