#include <atomic>
#include <chrono>
#include <ctime>
#include <future>
#include <iomanip>
#include <memory>
#include <thread>
#include <vector>

// submodules
#include "spdlog/spdlog.h"

// xtransmit
#include "socket_stats.hpp"
#include "srt_socket.hpp"
#include "srt_socket_group.hpp"
#include "udp_socket.hpp"
#include "receive.hpp"
#include "metrics.hpp"

// OpenSRT
#include "apputil.hpp"
#include "uriparser.hpp"

using namespace std;
using namespace xtransmit;
using namespace xtransmit::receive;
using namespace std::chrono;

using shared_srt  = std::shared_ptr<socket::srt>;
using shared_sock = std::shared_ptr<socket::isocket>;

#define LOG_SC_RECEIVE "RECEIVE "




void trace_message(const size_t bytes, const vector<char> &buffer, int conn_id)
{
	::cout << "RECEIVED MESSAGE length " << bytes << " on conn ID " << conn_id;

#if 0
	if (bytes < 50)
	{
		::cout << ":\n";
		::cout << string(buffer.data(), bytes).c_str();
	}
	else if (buffer[0] >= '0' && buffer[0] <= 'z')
	{
		::cout << " (first character):";
		::cout << buffer[0];
	}
#endif
	::cout << endl;

	//CHandShake hs;
	//if (hs.load_from(buffer.data(), buffer.size()) < 0)
	//	return;
	//
	//::cout << "SRT HS: " << hs.show() << endl;
}


void run_pipe(shared_sock src, const config &cfg, const atomic_bool &force_break)
{
	socket::isocket &sock = *src.get();

	vector<char> buffer(cfg.message_size);
	metrics::validator validator;

	auto stat_time = steady_clock::now();
	ofstream metrics_file;
	if (cfg.enable_metrics && !cfg.metrics_file.empty() && cfg.metrics_freq_ms > 0)
	{
		metrics_file.open(cfg.metrics_file, std::ofstream::out);
		if (!metrics_file)
		{
			spdlog::error(LOG_SC_RECEIVE "Failed to open metrics file {} for output", cfg.metrics_file);
			return;
		}

		metrics_file << validator.stats_csv(true);
	}

	try
	{
		while (!force_break)
		{
			const size_t bytes = sock.read(mutable_buffer(buffer.data(), buffer.size()), -1);

			if (bytes == 0)
			{
				spdlog::debug(LOG_SC_RECEIVE "sock::read() returned 0 bytes (spurious read ready?). Retrying.");
				continue;
			}

			if (cfg.print_notifications)
				trace_message(bytes, buffer, sock.id());
			if (cfg.enable_metrics)
				validator.validate_packet(buffer);

			if (cfg.send_reply)
			{
				const string out_message("Message received");
				sock.write(const_buffer(out_message.data(), out_message.size()));

				if (cfg.print_notifications)
					spdlog::error(LOG_SC_RECEIVE "Reply sent on conn ID {}", sock.id());
			}

			if (!cfg.enable_metrics)
				continue;

			const auto tnow = steady_clock::now();
			if (tnow > (stat_time + chrono::milliseconds(cfg.metrics_freq_ms)))
			{
				if (metrics_file)
				{
					metrics_file << validator.stats_csv(false);
				}
				else
				{
					const auto stats_str = validator.stats();
					spdlog::info(LOG_SC_RECEIVE "{}", stats_str);
				}
				stat_time = tnow;
			}
		}
	}
	catch (const socket::exception &e)
	{
		spdlog::warn(LOG_SC_RECEIVE "{}", e.what());
	}

	if (force_break)
	{
		spdlog::info(LOG_SC_RECEIVE "interrupted by request!");
	}
}

int srt_listen_callback(void* opaq, SRTSOCKET sock, int hsversion,
	const struct sockaddr* peeraddr, const char* streamid)
{
	spdlog::trace(LOG_SC_RECEIVE "Accepted member socket 0x{:X}.", sock);
	return 0;
}

void srt_connect_callback(void* opaq, SRTSOCKET sock, int error, const sockaddr* /*peer*/, int token)
{
	if (error != SRT_SUCCESS)
	{
		spdlog::warn(LOG_SC_RECEIVE "Member socket 0x{:X} (token {}) connection failed: ({}) {}.", sock, token, error,
			srt_strerror(error, 0));
		return;
	}

	// After SRT v1.4.2 connection callback is no longer called on connection success.
	spdlog::trace(LOG_SC_RECEIVE "Member socket connected 0x{:X} (token {}).", sock, token);
}

void xtransmit::receive::run(const vector<string> &src_urls, const config &cfg, const atomic_bool &force_break)
{
	if (src_urls.empty())
	{
		spdlog::error(LOG_SC_RECEIVE "No destination URI was provided");
		return;
	}

	vector<UriParser> urls;
	for (const string& url : src_urls)
	{
		urls.emplace_back(url);
	}

	unique_ptr<socket::stats_writer> stats;

	const bool write_stats = cfg.stats_file != "" && cfg.stats_freq_ms > 0;
	if (write_stats)
	{
		try {
			// make_unique is not supported by GCC 4.8, only starting from GCC 4.9 :(
			stats = unique_ptr<socket::stats_writer>(
				new socket::stats_writer(cfg.stats_file, milliseconds(cfg.stats_freq_ms)));
		}
		catch (const socket::exception& e)
		{
			spdlog::error(LOG_SC_RECEIVE "{}", e.what());
			return;
		}
	}

	do {
		try
		{
			shared_sock sock;
			shared_sock conn;

			if (urls.size() == 1)
			{
				if (urls[0].proto() == "udp")
				{
					conn = make_shared<socket::udp>(urls[0]);
				}
				else
				{
					sock = make_shared<socket::srt>(urls[0]);
					socket::srt* s = dynamic_cast<socket::srt*>(sock.get());
					const bool   accept = s->mode() == socket::srt::LISTENER;
					if (accept)
						s->listen();
					conn = accept ? s->accept() : s->connect();
				}
			}
			else
			{
				sock = make_shared<socket::srt_group>(urls);
				socket::srt_group* s = dynamic_cast<socket::srt_group*>(sock.get());
				const bool   accept = s->mode() == socket::srt_group::LISTENER;
				if (accept) {
					s->set_listen_callback(&srt_listen_callback, nullptr);
					s->listen();
				}
				else {
					s->set_connect_callback(&srt_connect_callback, nullptr);
				}
				conn = accept ? s->accept() : s->connect();
			}

			if (stats)
				stats->add_socket(conn);
			run_pipe(conn, cfg, force_break);
			if (stats && cfg.reconnect)
				stats->clear();
		}
		catch (const socket::exception & e)
		{
			spdlog::warn(LOG_SC_RECEIVE "{}", e.what());
			if (stats)
				stats->clear();
		}
	} while (cfg.reconnect && !force_break);
}

CLI::App* xtransmit::receive::add_subcommand(CLI::App& app, config& cfg, vector<string>& src_urls)
{
	const map<string, int> to_ms{ {"s", 1000}, {"ms", 1} };

	CLI::App* sc_receive = app.add_subcommand("receive", "Receive data (SRT, UDP)")->fallthrough();
	sc_receive->add_option("--input,-i,src", src_urls, "Source URI");
	sc_receive->add_option("--msgsize", cfg.message_size, "Size of a buffer to receive message payload");
	sc_receive->add_option("--statsfile", cfg.stats_file, "output stats report filename");
	sc_receive->add_option("--statsfreq", cfg.stats_freq_ms, "output stats report frequency (ms)")
		->transform(CLI::AsNumberWithUnit(to_ms, CLI::AsNumberWithUnit::CASE_SENSITIVE));
	sc_receive->add_flag("--printmsg", cfg.print_notifications, "print message into to stdout");
	sc_receive->add_flag("--reconnect", cfg.reconnect, "Reconnect automatically");
	sc_receive->add_flag("--enable-metrics", cfg.enable_metrics, "Enable checking metrics: jitter, latency, etc.");
	sc_receive->add_option("--metricsfile", cfg.metrics_file, "Metrics output filename (stdout if not set)");
	sc_receive->add_option("--metricsfreq", cfg.metrics_freq_ms, "Metrics report frequency")
		->transform(CLI::AsNumberWithUnit(to_ms, CLI::AsNumberWithUnit::CASE_SENSITIVE));
	sc_receive->add_flag("--twoway", cfg.send_reply, "Both send and receive data");
	sc_receive->add_option("--input-group", cfg.inputs, "More input group URLs for SRT bonding");

	return sc_receive;
}



