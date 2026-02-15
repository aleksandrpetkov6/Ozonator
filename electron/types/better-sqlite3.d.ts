// Minimal TypeScript declarations for `better-sqlite3`.
// Goal: make `tsc --noEmit` pass in CI.
// If you later want strong typing, replace this file with full typings (e.g., @types/better-sqlite3).

declare module 'better-sqlite3' {
  // The real typings expose a callable/constructable export with an attached namespace `Database`
  // (so code can use `Database.Database`, `Database.Statement`, etc.).
  namespace Database {
    interface Database {
      prepare(sql: string): Statement;
      exec(sql: string): void;
      close(): void;
      pragma?(source: string, options?: any): any;
      transaction?<T extends (...args: any[]) => any>(fn: T): T;
    }

    interface Statement {
      run(...params: any[]): RunResult;
      get(...params: any[]): any;
      all(...params: any[]): any[];
      iterate?(...params: any[]): IterableIterator<any>;
      pluck?(toggleState?: boolean): this;
      raw?(toggleState?: boolean): this;
      expand?(toggleState?: boolean): this;
    }

    interface RunResult {
      changes: number;
      lastInsertRowid: number | bigint;
    }

    interface Options {
      readonly?: boolean;
      fileMustExist?: boolean;
      timeout?: number;
      verbose?: (...args: any[]) => void;
    }
  }

  interface DatabaseConstructor {
    new (path: string, options?: Database.Options): Database.Database;
    (path: string, options?: Database.Options): Database.Database;
  }

  const Database: DatabaseConstructor;
  export = Database;
}
