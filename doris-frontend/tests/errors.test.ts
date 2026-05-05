import test from 'node:test';
import assert from 'node:assert/strict';

import { extractApiErrorMessage } from '../src/api/errors.ts';

test('extractApiErrorMessage returns nested detail.error when present', () => {
  const error = {
    response: {
      data: {
        detail: {
          error: 'append mode requires matching columns',
        },
      },
    },
    message: 'Request failed with status code 400',
  };

  assert.equal(extractApiErrorMessage(error), 'append mode requires matching columns');
});

test('extractApiErrorMessage returns string detail when present', () => {
  const error = {
    response: {
      data: {
        detail: 'system tables cannot be deleted via this API',
      },
    },
    message: 'Request failed with status code 400',
  };

  assert.equal(extractApiErrorMessage(error), 'system tables cannot be deleted via this API');
});

test('extractApiErrorMessage returns nested detail.message when present', () => {
  const error = {
    response: {
      data: {
        detail: {
          message: 'Packet sequence number wrong - got 80 expected 0',
        },
      },
    },
    message: 'Request failed with status code 500',
  };

  assert.equal(extractApiErrorMessage(error), 'Packet sequence number wrong - got 80 expected 0');
});
