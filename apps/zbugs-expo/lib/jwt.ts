import {decodeJwt} from 'jose';
import Cookies from 'js-cookie';

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
  return Cookies.get('jwt');
}

export function clearJwt() {
  deleteCookie('jwt');
}

function deleteCookie(name: string) {
  Cookies.remove(name);
}
