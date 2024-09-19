import {decodeJwt} from 'jose';

export function getJwt() {
  const cookies = document.cookie.split(';');
  const jwtCookie = cookies.find(cookie => cookie.trim().startsWith('jwt='));

  if (!jwtCookie) {
    console.log('No jwt cookie found');
    return undefined;
  }

  const token = jwtCookie.split('=')[1].trim();
  const payload = decodeJwt(token);
  const currentTime = Math.floor(Date.now() / 1000);
  if (payload.exp && payload.exp < currentTime) {
    console.log('JWT token has expired');
    return undefined;
  }

  console.log('JWT token found');
  return payload;
}

export function clearJwt() {
  deleteCookie('jwt');
}

function deleteCookie(name: string) {
  document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;`;
}
