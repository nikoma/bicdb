require "net/http"
require "uri"
require "nokogiri"
require "json"
require "socket"
require "timeout"
require "websocket/driver"

base = URI(ENV.fetch("TEAM_BOARD_HTTP_URL"))
cookies = {}
request = lambda do |method, path, data = nil|
  path = URI(path).request_uri if path.start_with?("http://", "https://")
  klass = Net::HTTP.const_get(method.capitalize)
  req = klass.new(path)
  req["Cookie"] = cookies.map { |key,value| "#{key}=#{value}" }.join("; ")
  req.set_form_data(data) if data
  response = Net::HTTP.start(base.host, base.port) { |http| http.request(req) }
  response.get_fields("set-cookie").to_a.each do |cookie|
    key,value = cookie.split(";", 2).first.split("=", 2)
    cookies[key] = value
  end
  response
end
check = ->(condition, message) { raise message unless condition }
response = request.call("get", "/session/new")
check.call(response.code == "200", "login page failed: #{response.code}")
token = Nokogiri::HTML(response.body).at_css('input[name="authenticity_token"]')["value"]
response = request.call("post", "/session", { "email" => "demo@example.test", "password" => "demo-password-please-change", "authenticity_token" => token })
check.call(response.is_a?(Net::HTTPRedirection), "real CSRF-protected login failed")
response = request.call("get", "/")
check.call(response.code == "200" && response.body.include?("Product launch"), "project index failed")
html = Nokogiri::HTML(response.body)
path = html.at_css("a.project-card")["href"]
response = request.call("get", path)
check.call(response.code == "200" && response.body.include?("Make search feel instant"), "board failed")
check.call(response.body.include?("turbo-cable-stream-source"), "live stream subscription missing")
board_html = Nokogiri::HTML(response.body)
signed_stream = board_html.at_css("turbo-cable-stream-source")["signed-stream-name"]
report_token = board_html.at_css("form[action='#{path}/reports'] input[name='authenticity_token']")["value"]
assets = Nokogiri::HTML(response.body).css('link[rel="stylesheet"]').map { |node| node["href"] }
assets.each { |asset| check.call(request.call("get", asset).code == "200", "stylesheet failed") }
response = request.call("get", "#{path}?q=accessibility")
check.call(response.code == "200" && response.body.include?("Review accessibility"), "full-text search failed")
check.call(!response.body.include?("Ship the onboarding checklist"), "search returned unrelated issue")
response = request.call("get", "#{path}/issues/new")
html = Nokogiri::HTML(response.body)
token = html.at_css("form[action='#{path}/issues'] input[name='authenticity_token']")["value"]
response = request.call("post", "#{path}/issues", { "authenticity_token" => token,
  "issue[title]" => "Verified through real HTTP", "issue[body]" => "HTTP smoke test", "issue[priority]" => "2", "issue[status]" => "active" })
check.call(response.is_a?(Net::HTTPRedirection), "issue creation failed: #{response.code}")
response = request.call("get", response["location"])
check.call(response.code == "200" && response.body.include?("Verified through real HTTP"), "saved issue page failed")
puts "Real HTTP login, CSRF, project board, assets, full-text search and issue creation passed."

# Subscribe over an actual WebSocket, then trigger a real background report.
transport_class = Struct.new(:url, :socket) do
  def write(data)
    socket.write(data)
  end
end
socket = TCPSocket.new(base.host, base.port)
transport = transport_class.new("ws://#{base.host}:#{base.port}/cable", socket)
driver = WebSocket::Driver.client(transport, protocols: ["actioncable-v1-json"])
driver.set_header("Origin", "http://#{base.host}:#{base.port}")
driver.set_header("Cookie", cookies.map { |key,value| "#{key}=#{value}" }.join("; "))
messages = []
identifier = JSON.generate(channel: "Turbo::StreamsChannel", signed_stream_name: signed_stream)
driver.on(:message) do |event|
  message = JSON.parse(event.data)
  if message["type"] == "welcome"
    driver.text(JSON.generate(command: "subscribe", identifier: identifier))
  end
  messages << message
end
driver.on(:error) { |error| raise error.message }
begin
  driver.start
  Timeout.timeout(30) do
    driver.parse(socket.readpartial(65536)) until messages.any? { |message| message["type"] == "confirm_subscription" }
  end
  response = request.call("post", "#{path}/reports", { "authenticity_token" => report_token })
  check.call(response.is_a?(Net::HTTPRedirection), "report HTTP enqueue failed")
  Timeout.timeout(60) do
    until messages.any? { |message| message["message"].to_s.include?("The latest snapshot") }
      driver.parse(socket.readpartial(65536))
    end
  end
  puts "Solid Cable delivered the worker's Turbo update over a real WebSocket."
ensure
  driver.close
  socket.close
end
