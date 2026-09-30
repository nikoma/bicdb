local raw_call = redis.call
local call = function(method, args)
  return cjson.decode(raw_call('__WORKFLOW', method, cjson.encode(args)))
end
local sql = function(prefix)
  return {
    one = function(statement, parameters) return call(prefix .. '.one', {statement, parameters or cjson.decode('[]')}) end,
    scalar = function(statement, parameters) return call(prefix .. '.scalar', {statement, parameters or cjson.decode('[]')}) end,
    execute = function(statement, parameters) return call(prefix .. '.execute', {statement, parameters or cjson.decode('[]')}) end
  }
end
db = sql('db')
db.transaction = function(callback)
  call('db.begin', cjson.decode('[]'))
  local ok, result = pcall(callback, sql('tx'))
  if not ok then
    call('db.rollback', cjson.decode('[]'))
    error(result)
  end
  local committed, reason = pcall(call, 'db.commit', cjson.decode('[]'))
  if not committed then
    call('db.rollback', cjson.decode('[]'))
    error(reason)
  end
  return result
end
http = {post = function(url, options) return call('http.post', {url, options}) end}
secrets = {get = function(name) return call('secrets.get', {name}) end}
jobs = {retry = function(options) return call('jobs.retry', {options}) end}
json = cjson
event = cjson.decode(ARGV[1])
