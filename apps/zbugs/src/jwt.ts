import {decodeJwt} from 'jose';

export function getJwt() {
  const token = getRawJwt();
  if (!token) {
    return undefined;
  }
  const payload = decodeJwt(token);
  const currentTime = Math.floor(Date.now() / 1000);
  if (payload.exp && payload.exp < currentTime) {
    return undefined;
  }

  return payload;
}

export function getRawJwt() {
  const cookies = document.cookie.split(';');
  const jwtCookie = cookies.find(cookie => cookie.trim().startsWith('jwt='));

  if (!jwtCookie) {
    return undefined;
  }

  return jwtCookie.split('=')[1].trim();
}

export function clearJwt() {
  deleteCookie('jwt');
}

function deleteCookie(name: string) {
  document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;`;
}
