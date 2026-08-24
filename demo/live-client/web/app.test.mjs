import test from 'node:test';
import assert from 'node:assert/strict';
import { canSendCaptureFrame, sessionReadinessAfter, socketTerminationReason, statusLabel } from './app.js';

test('session readiness waits for an acknowledgement after the update is sent', () => {
  const initial = { updateSent: false, configured: false };
  const beforeCreation = sessionReadinessAfter(initial, 'session.updated');
  const updateSent = sessionReadinessAfter(initial, 'session.created');
  const acknowledged = sessionReadinessAfter(updateSent, 'session.updated');

  assert.deepEqual(beforeCreation, { updateSent: false, configured: false, sendUpdate: false, enableCapture: false });
  assert.deepEqual(updateSent, { updateSent: true, configured: false, sendUpdate: true, enableCapture: false });
  assert.deepEqual(acknowledged, { updateSent: true, configured: true, sendUpdate: false, enableCapture: true });
});

test('capture frames cannot send before session acknowledgement', () => {
  const message = { type: 'frame', pcm: new ArrayBuffer(960) };

  assert.equal(canSendCaptureFrame({ active: true, configured: false, muted: false, socketOpen: true, message }), false);
  assert.equal(canSendCaptureFrame({ active: true, configured: true, muted: false, socketOpen: true, message }), true);
});

test('startup failure label remains available to the rendered event list', () => {
  assert.equal(statusLabel({ kind: 'start-failed' }), 'Unable to start the local voice session.');
});

test('pre-acknowledgement socket termination is shown as an unavailable session', () => {
  assert.equal(socketTerminationReason({ configured: false }), 'session-unavailable');
  assert.equal(statusLabel({ kind: 'session-unavailable' }), 'Local voice session is unavailable. Select Start to retry.');
  assert.equal(socketTerminationReason({ configured: true }), 'connection-closed');
});
