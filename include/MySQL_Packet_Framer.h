#ifndef PROXYSQL_MYSQL_PACKET_FRAMER_H
#define PROXYSQL_MYSQL_PACKET_FRAMER_H

#include <algorithm>
#include <cstddef>
#include <cstdint>

/**
 * @brief Follows a stream of MySQL packets and tells where a block of it ends inside a packet header.
 * @details We want the compressed packets that we send to a client to end between the bytes of a
 *  MySQL packet, or in its payload, but never inside its 4 byte header. The protocol allows any cut, but
 *  some drivers (mysql-connector-python) read the length of the next packet from the 3 bytes after
 *  the cut and fail when only 1 or 2 of them are there. A block that comes from the result spill
 *  has an arbitrary size, so we call feed() for every block, in the order of the stream. The caller
 *  holds back the bytes that feed() returns and puts them in front of the next block.
 */
class MySQL_Packet_Framer {
	public:
	/**
	 * @brief Reads the next block of the stream.
	 * @return The number of bytes at the end of the block that are the first 1 to 3 bytes of a packet header.
	 *  The framer then expects those bytes in front of the next block, so the caller must hold them back.
	 *  Zero when the block ends at a packet boundary or inside a payload.
	 */
	size_t feed(const unsigned char *data, size_t length) {
		size_t position = 0;
		while (position < length) {
			if (header_have < header_size) {
				header[header_have++] = data[position++];
				if (header_have == header_size) {
					payload_remaining = static_cast<uint32_t>(header[0]) | (static_cast<uint32_t>(header[1]) << 8) |
						(static_cast<uint32_t>(header[2]) << 16);
					if (payload_remaining == 0) header_have = 0;
				}
			} else {
				const size_t skipped = std::min<size_t>(payload_remaining, length - position);
				position += skipped;
				payload_remaining -= static_cast<uint32_t>(skipped);
				if (payload_remaining == 0) header_have = 0;
			}
		}
		if (header_have > 0 && header_have < header_size) {
			const size_t held = header_have;
			reset();
			return held;
		}
		return 0;
	}

	/// The next block starts at a packet boundary.
	void reset() {
		header_have = 0;
		payload_remaining = 0;
	}

	private:
	static constexpr uint32_t header_size = 4;
	unsigned char header[header_size] = {0, 0, 0, 0};
	uint32_t header_have = 0;
	uint32_t payload_remaining = 0;
};

#endif /* PROXYSQL_MYSQL_PACKET_FRAMER_H */
