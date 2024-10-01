/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        primary: '#ff5c00',
        text: '#ffffff',
        modal: '#121212',
        modalOutline: '#3e3e3e',
      },
    },
  },
  plugins: [],
};
