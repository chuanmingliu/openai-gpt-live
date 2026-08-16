import {decodePublishedList} from './profile-api.mjs';

export const BUILTIN_PROFILE = Object.freeze({id: 'builtin', version: 1});

const HASH_PATTERN = /^[0-9a-f]{64}$/;
const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;

function validSelection(value) {
  return value !== null && typeof value === 'object'
    && typeof value.id === 'string' && ID_PATTERN.test(value.id)
    && typeof value.version === 'number' && Number.isSafeInteger(value.version) && value.version >= 1;
}

// createVoiceProfileState owns the published-profile options, the immutable
// pre-connect selection, and the capture acknowledgement gate. Selection lives
// only in memory; a custom selection requires an exact live.profile.selected
// acknowledgement before capture may start, and any mismatch fails closed.
export function createVoiceProfileState(initial = BUILTIN_PROFILE) {
  if (!validSelection(initial)) throw new TypeError('invalid initial profile selection');
  let options = Object.freeze([BUILTIN_PROFILE]);
  let selection = Object.freeze({id: initial.id, version: initial.version});
  let locked = false;
  let profileAcked = false;
  let providerAcked = false;
  let failure = '';

  function isBuiltin() {
    return selection.id === BUILTIN_PROFILE.id;
  }

  return Object.freeze({
    // loadPublished replaces the selectable options from the safe published
    // list. Revoked versions are never selectable; builtin is always first.
    loadPublished(rawList) {
      const decoded = decodePublishedList(rawList);
      const selectable = [BUILTIN_PROFILE];
      for (const entry of decoded) {
        if (!entry.revoked) selectable.push(Object.freeze({id: entry.id, version: entry.version, active: entry.active}));
      }
      options = Object.freeze(selectable);
      if (!locked && !options.some((option) => option.id === selection.id && option.version === selection.version)) {
        selection = Object.freeze({...BUILTIN_PROFILE});
      }
      return options;
    },
    options() {
      return options;
    },
    select(next) {
      if (locked) throw new Error('selection_locked');
      if (!validSelection(next) || !options.some((option) => option.id === next.id && option.version === next.version)) {
        throw new Error('selection_unavailable');
      }
      selection = Object.freeze({id: next.id, version: next.version});
      return selection;
    },
    selection() {
      return selection;
    },
    // query returns the exact /ws query for the current selection. The builtin
    // default sends no profile pair so voice-only gateways stay compatible.
    query() {
      if (isBuiltin()) return '';
      return `profile=${encodeURIComponent(selection.id)}&profile_version=${selection.version}`;
    },
    onConnected() {
      locked = true;
      profileAcked = false;
      providerAcked = false;
      failure = '';
    },
    onDisconnected() {
      locked = false;
      profileAcked = false;
      providerAcked = false;
      failure = '';
    },
    onProfileSelected(ack) {
      if (failure !== '') return false;
      if (ack === null || typeof ack !== 'object'
          || typeof ack.id !== 'string' || typeof ack.version !== 'number'
          || typeof ack.snapshot_hash !== 'string' || !HASH_PATTERN.test(ack.snapshot_hash)) {
        failure = 'profile_mismatch';
        return false;
      }
      if (ack.id !== selection.id || ack.version !== selection.version) {
        failure = 'profile_mismatch';
        return false;
      }
      profileAcked = true;
      return true;
    },
    onProviderUpdated() {
      providerAcked = true;
    },
    captureReady() {
      if (failure !== '') return false;
      if (!providerAcked) return false;
      return isBuiltin() || profileAcked;
    },
    // profileAckPending reports whether a custom selection still awaits its
    // exact acknowledgement, so callers can bound the wait.
    profileAckPending() {
      return !isBuiltin() && !profileAcked && failure === '';
    },
    failureClass() {
      return failure;
    },
  });
}
