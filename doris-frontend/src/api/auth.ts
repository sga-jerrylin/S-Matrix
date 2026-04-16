export const STORAGE_KEY = 'smatrix_api_key';

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

interface ResolveGatewayApiKeyOptions {
  envApiKey?: string;
  storage?: StorageLike;
}

const normalizeApiKey = (value?: string | null): string => (value || '').trim();

const getBrowserStorage = (): StorageLike | undefined => {
  if (typeof window === 'undefined') {
    return undefined;
  }

  try {
    return window.localStorage;
  } catch {
    return undefined;
  }
};

export const resolveGatewayApiKey = (options: ResolveGatewayApiKeyOptions = {}): string => {
  const storage = options.storage ?? getBrowserStorage();
  const storedValue = normalizeApiKey(storage?.getItem(STORAGE_KEY));
  if (storedValue) {
    return storedValue;
  }
  return normalizeApiKey(options.envApiKey);
};

export const setStoredGatewayApiKey = (
  apiKey: string,
  storage: StorageLike | undefined = getBrowserStorage(),
): string => {
  const normalizedApiKey = normalizeApiKey(apiKey);
  if (storage) {
    if (normalizedApiKey) {
      storage.setItem(STORAGE_KEY, normalizedApiKey);
    } else {
      storage.removeItem(STORAGE_KEY);
    }
  }
  return normalizedApiKey;
};

export const clearStoredGatewayApiKey = (storage: StorageLike | undefined = getBrowserStorage()): void => {
  storage?.removeItem(STORAGE_KEY);
};

export const applyGatewayAuthHeaders = (
  headers: Record<string, string | undefined>,
  apiKey: string,
): Record<string, string | undefined> => {
  const nextHeaders = { ...headers };
  const normalizedApiKey = normalizeApiKey(apiKey);

  if (normalizedApiKey) {
    nextHeaders['X-API-Key'] = normalizedApiKey;
    nextHeaders.Authorization = `Bearer ${normalizedApiKey}`;
  } else {
    delete nextHeaders['X-API-Key'];
    delete nextHeaders.Authorization;
  }

  return nextHeaders;
};
