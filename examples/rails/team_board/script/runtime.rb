require "fileutils"
require "rbconfig"
require "socket"
require "timeout"

module DemoRuntime
  ROOT = File.expand_path("..", __dir__)
  REPOSITORY = File.expand_path("../../..", ROOT)

  def self.dependencies!
    Dir.chdir(ROOT)
    bundle = [RbConfig.ruby, Gem.bin_path("bundler", "bundle", ">= 2.6")]
    system(*bundle, "check", out: File::NULL) || system(*bundle, "install", exception: true)
    require "bundler/setup"
  end

  def self.binary!
    if ENV["BICDB_BIN"]
      binary = File.expand_path(ENV.fetch("BICDB_BIN"))
    else
      system("cargo", "build", "--release", "--locked", "-p", "bicdb-cli", "--bin", "bicdb",
        chdir: REPOSITORY, exception: true)
      target = File.expand_path(ENV.fetch("CARGO_TARGET_DIR", "target"), REPOSITORY)
      binary = File.join(target, "release", "bicdb")
    end
    abort "BICDB_BIN is not executable: #{binary}" unless File.executable?(binary)
    binary
  end

  def self.free_port
    TCPServer.open("127.0.0.1", 0) { |server| server.addr[1] }
  end

  class Processes
    attr_reader :directory, :binary, :port

    def initialize(directory, binary, port: nil)
      @directory, @binary = File.expand_path(directory), binary
      @port = port || DemoRuntime.free_port
      @children = {}
      FileUtils.mkdir_p(@directory)
      # These scripts own a fresh/local server; inherited Rails URL overrides
      # must never redirect preparation or rollback commands to another database.
      %w[DATABASE_URL PRIMARY_DATABASE_URL QUEUE_DATABASE_URL CACHE_DATABASE_URL CABLE_DATABASE_URL].each { |name| ENV.delete(name) }
      ENV["BICDB_SSLMODE"] = "disable"
      ENV["BICDB_HOST"] = "127.0.0.1"
      ENV["BICDB_PORT"] = @port.to_s
    end

    def start(name, *command, env: {})
      log = File.open(File.join(directory, "#{name}.log"), "a")
      pid = Process.spawn(env, *command, chdir: ROOT, pgroup: true, out: log, err: [:child, :out])
      log.close
      @children[pid] = name
      pid
    end

    def command(name, *command, env: {})
      log = File.join(directory, "#{name}.log")
      ok = File.open(log, "w") do |output|
        system(env, *command, chdir: ROOT, out: output, err: [:child, :out])
      end
      abort "#{name} failed; see #{log}\n#{File.read(log)[-12000..] || File.read(log)}" unless ok
      puts "#{name}: passed"
    end

    def rails(name, *args, env: {})
      command(name, RbConfig.ruby, "bin/rails", *args, env: env)
    end

    def database
      log_path = File.join(directory, "bicdb.log")
      log_offset = File.exist?(log_path) ? File.size(log_path) : 0
      @database = start("bicdb", binary, "serve", File.join(directory, "cluster"),
        "--cluster", "--default-database", "postgres", "--host", "127.0.0.1", "--port", port.to_s,
        "--max-connections-per-ip", "100", "--max-connections", "150")
      require "pg"
      Timeout.timeout(60) do
        loop do
          if Process.waitpid(@database, Process::WNOHANG)
            abort "BicDB exited; see #{directory}/bicdb.log"
          end
          # Wait for this child's successful bind before connecting. An occupied
          # port must never redirect setup to a different, already-running server.
          listening = File.open(log_path) do |log|
            log.seek(log_offset)
            log.read.include?('"event":"server.listening"')
          end
          unless listening
            sleep 0.1
            next
          end
          begin
            connection = PG.connect(host: "127.0.0.1", port: port, user: ENV.fetch("BICDB_USER", "bicdb"),
              password: ENV["BICDB_PASSWORD"], dbname: "postgres", sslmode: "disable", connect_timeout: 2)
            connection.close
            break
          rescue PG::ConnectionBad
            sleep 0.1
          end
        end
      end
      puts "BicDB ready on 127.0.0.1:#{port}"
    end

    def restart_database(crash: false)
      stop(@database, signal: crash ? "KILL" : "TERM")
      database
    end

    def stop(pid, signal: "TERM")
      return unless @children.delete(pid)
      begin
        Process.kill(signal, -pid)
      rescue Errno::ESRCH
      end
      begin
        Timeout.timeout(20) { Process.waitpid(pid) }
      rescue Timeout::Error
        Process.kill("KILL", -pid) rescue Errno::ESRCH
        Process.waitpid(pid) rescue Errno::ECHILD
      rescue Errno::ECHILD
      end
    end

    def wait
      pid, status = Process.wait2
      @children.delete(pid)
      abort "A demo process exited (#{status}); see #{directory}" unless status.success?
    end

    def close
      @children.keys.reverse_each { |pid| stop(pid) }
    end
  end
end
