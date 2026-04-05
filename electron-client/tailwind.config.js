/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx,ts,tsx}"],
  theme: {
    extend: {
      colors: {
        brand: {
          50: "#eef9ff",
          100: "#d8f1ff",
          200: "#b9e8ff",
          300: "#89daff",
          400: "#53c3ff",
          500: "#2ba5ff",
          600: "#0d85f5",
          700: "#0d6ed9",
          800: "#1158ae",
          900: "#144b8a",
          950: "#112f57",
        },
      },
    },
  },
  plugins: [],
};
