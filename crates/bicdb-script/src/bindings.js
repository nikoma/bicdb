"use strict";
const event = JSON.parse(__event);
const call = (method, args) => {
  const response = JSON.parse(__host(method, JSON.stringify(args)));
  if (Object.hasOwn(response, "error")) throw new Error(response.error);
  return response.value;
};
const sql = prefix => Object.freeze({
  one: (statement, parameters = []) => call(prefix + ".one", [statement, parameters]),
  scalar: (statement, parameters = []) => call(prefix + ".scalar", [statement, parameters]),
  execute: (statement, parameters = []) => call(prefix + ".execute", [statement, parameters]),
});
const db = Object.freeze({
  ...sql("db"),
  transaction: callback => {
    call("db.begin", []);
    try {
      const result = callback(sql("tx"));
      if (result && typeof result.then === "function") throw new Error("Transaction callbacks must be synchronous");
      call("db.commit", []);
      return result;
    } catch (error) {
      call("db.rollback", []);
      throw error;
    }
  },
});
const http = Object.freeze({post: (url, options) => call("http.post", [url, options])});
const secrets = Object.freeze({get: name => call("secrets.get", [name])});
const jobs = Object.freeze({retry: options => call("jobs.retry", [options])});
const json = Object.freeze({encode: JSON.stringify, decode: JSON.parse});
