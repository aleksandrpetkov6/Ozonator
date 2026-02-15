// Minimal TypeScript declarations to satisfy `tsc --noEmit` in CI.
// If you later want strong typing, replace this with @types/better-sqlite3 or full definitions.

declare module 'better-sqlite3' {
  interface DatabaseConstructor {
    new (...args: any[]): any;
    (...args: any[]): any;
  }

  const Database: DatabaseConstructor;
  export = Database;
}
