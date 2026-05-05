export const extractApiErrorMessage = (error: any): string => {
  const nestedError = error?.response?.data?.detail?.error;
  if (typeof nestedError === 'string' && nestedError.trim()) {
    return nestedError;
  }

  const detail = error?.response?.data?.detail;
  if (detail && typeof detail === 'object') {
    const detailMessage = detail.message || detail.error;
    if (typeof detailMessage === 'string' && detailMessage.trim()) {
      return detailMessage;
    }
  }

  if (typeof detail === 'string' && detail.trim()) {
    return detail;
  }

  const responseMessage = error?.response?.data?.message || error?.response?.data?.error;
  if (typeof responseMessage === 'string' && responseMessage.trim()) {
    return responseMessage;
  }

  if (typeof error?.message === 'string' && error.message.trim()) {
    return error.message;
  }

  return '未知错误';
};
