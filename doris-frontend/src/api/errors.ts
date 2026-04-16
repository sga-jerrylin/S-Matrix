export const extractApiErrorMessage = (error: any): string => {
  const nestedError = error?.response?.data?.detail?.error;
  if (typeof nestedError === 'string' && nestedError.trim()) {
    return nestedError;
  }

  const detail = error?.response?.data?.detail;
  if (typeof detail === 'string' && detail.trim()) {
    return detail;
  }

  if (typeof error?.message === 'string' && error.message.trim()) {
    return error.message;
  }

  return '未知错误';
};
