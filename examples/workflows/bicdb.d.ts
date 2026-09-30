// Editor/type-checker declarations. Implemented by the capability workflow host.
type SqlParameters = readonly (string | number | boolean | null | object)[];
interface BicDbTransaction {
  one<T = Record<string, any>>(statement: string, parameters?: SqlParameters): T;
  scalar<T = unknown>(statement: string, parameters?: SqlParameters): T;
  execute(statement: string, parameters?: SqlParameters): number | Record<string, unknown>[];
}
declare const db: BicDbTransaction & {
  transaction<T>(callback: (tx: BicDbTransaction) => T): T;
};
declare const http: {
  post(url: string, options: {
    headers?: Record<string, string>;
    json?: unknown;
    timeout_ms?: number;
  }): {status: number; body: string};
};
declare const secrets: {get(name: string): string};
declare const jobs: {retry(options: {delay_seconds: number}): void};
declare const json: {encode(value: unknown): string; decode<T = any>(value: string): T};
