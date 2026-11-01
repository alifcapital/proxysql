#include "MySQL_Packet_Framer.h"
#include "tap.h"
#include "test_globals.h"
#include "test_init.h"

#include <cstdint>
#include <random>
#include <vector>

// A stream of MySQL packets with the given payload sizes.
static std::vector<unsigned char> build_stream(const std::vector<uint32_t>& payloads, std::vector<size_t>& starts) {
	std::vector<unsigned char> stream;
	uint8_t sequence = 0;
	for (uint32_t size : payloads) {
		starts.push_back(stream.size());
		stream.push_back(size & 0xff);
		stream.push_back((size >> 8) & 0xff);
		stream.push_back((size >> 16) & 0xff);
		stream.push_back(sequence++);
		for (uint32_t i = 0; i < size; ++i) stream.push_back(static_cast<unsigned char>(i * 31 + size));
	}
	return stream;
}

// Cuts the stream into blocks like the result spill does, runs them through the framer the way
// MySQL_Data_Stream::spill_output() does, and returns the blocks that would be compressed.
static std::vector<std::vector<unsigned char>> frame(const std::vector<unsigned char>& stream, std::mt19937& random,
	size_t max_block) {
	MySQL_Packet_Framer framer;
	std::vector<unsigned char> carry;
	std::vector<std::vector<unsigned char>> blocks;
	size_t position = 0;
	while (position < stream.size()) {
		const size_t length = std::min<size_t>(1 + random() % max_block, stream.size() - position);
		std::vector<unsigned char> data(stream.begin() + position, stream.begin() + position + length);
		position += length;
		data.insert(data.begin(), carry.begin(), carry.end());
		carry.clear();
		const size_t held = framer.feed(data.data(), data.size());
		if (held) {
			carry.assign(data.end() - held, data.end());
			data.resize(data.size() - held);
		}
		if (!data.empty()) blocks.push_back(std::move(data));
	}
	if (!carry.empty()) blocks.push_back(carry);
	return blocks;
}

// Number of blocks that end with 1 to 3 bytes of a packet header.
static size_t blocks_cut_in_header(const std::vector<std::vector<unsigned char>>& blocks, const std::vector<size_t>& starts) {
	size_t position = 0, bad = 0, next = 0;
	for (const auto& block : blocks) {
		position += block.size();
		while (next < starts.size() && starts[next] < position) ++next;
		// 'next' is the first packet that starts at or after the end of the block; the one before it holds the cut.
		if (next > 0) {
			const size_t inside = position - starts[next - 1];
			if (inside >= 1 && inside <= 3) ++bad;
		}
	}
	return bad;
}

int main() {
	plan(7);
	test_init_minimal();
	std::mt19937 random(20261102);

	// Rows of 1 to 20 bytes: a block ends in a header very often without the framer.
	std::vector<uint32_t> payloads;
	for (int i = 0; i < 20000; ++i) payloads.push_back(1 + random() % 20);
	std::vector<size_t> starts;
	const auto stream = build_stream(payloads, starts);

	size_t cut_in_header_without_framer = 0;
	{
		std::mt19937 r(1);
		size_t position = 0, next = 0;
		while (position < stream.size()) {
			position += std::min<size_t>(1 + r() % 64, stream.size() - position);
			while (next < starts.size() && starts[next] < position) ++next;
			if (next > 0 && position - starts[next - 1] >= 1 && position - starts[next - 1] <= 3) ++cut_in_header_without_framer;
		}
	}
	ok(cut_in_header_without_framer > 100, "blocks of an arbitrary size end inside a packet header (%zu of them)", cut_in_header_without_framer);

	const auto blocks = frame(stream, random, 64);
	ok(blocks_cut_in_header(blocks, starts) == 0, "no framed block ends inside a packet header");
	std::vector<unsigned char> joined;
	for (const auto& block : blocks) joined.insert(joined.end(), block.begin(), block.end());
	ok(joined == stream, "the framed blocks together are the original stream, byte for byte");

	// A packet larger than the blocks: cuts fall in its payload and are allowed.
	std::vector<size_t> big_starts;
	const auto big = build_stream({3, 200000, 0, 5, 70000}, big_starts);
	const auto big_blocks = frame(big, random, 1000);
	std::vector<unsigned char> big_joined;
	for (const auto& block : big_blocks) big_joined.insert(big_joined.end(), block.begin(), block.end());
	ok(blocks_cut_in_header(big_blocks, big_starts) == 0 && big_joined == big, "blocks inside large payloads are kept whole");

	// One byte at a time: every block is a whole packet or a part of a payload.
	std::vector<size_t> small_starts;
	const auto small = build_stream({0, 1, 0, 2, 7, 0}, small_starts);
	const auto byte_blocks = frame(small, random, 1);
	std::vector<unsigned char> small_joined;
	for (const auto& block : byte_blocks) small_joined.insert(small_joined.end(), block.begin(), block.end());
	ok(blocks_cut_in_header(byte_blocks, small_starts) == 0 && small_joined == small, "one byte blocks, empty packets included");

	// The result of feed() for a block that ends at a boundary or in a payload is zero.
	MySQL_Packet_Framer framer;
	const unsigned char packet[] = {2, 0, 0, 0, 'a', 'b'};
	ok(framer.feed(packet, sizeof(packet)) == 0, "a block with whole packets holds nothing back");
	const unsigned char partial[] = {2, 0};
	ok(framer.feed(partial, sizeof(partial)) == 2, "two bytes of a header are held back");

	test_cleanup_minimal();
	return exit_status();
}
