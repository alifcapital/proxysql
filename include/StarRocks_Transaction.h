#ifndef PROXYSQL_STARROCKS_TRANSACTION_H
#define PROXYSQL_STARROCKS_TRANSACTION_H

#include <cctype>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

// Returns the first words of a statement in upper case, skipping whitespace and comments.
inline std::vector<std::string> starrocks_leading_words(const char* sql, size_t length, size_t limit) {
	std::vector<std::string> words;
	for (size_t pos = 0; pos < length && words.size() < limit;) {
		if (isspace(static_cast<unsigned char>(sql[pos]))) { ++pos; continue; }
		if (pos + 1 < length && sql[pos] == '/' && sql[pos+1] == '*') {
			pos += 2;
			while (pos + 1 < length && !(sql[pos] == '*' && sql[pos+1] == '/')) ++pos;
			pos += 2; continue;
		}
		if (sql[pos] == '#' || (pos + 1 < length && sql[pos] == '-' && sql[pos+1] == '-')) {
			while (pos < length && sql[pos] != '\n') ++pos;
			continue;
		}
		if (!isalpha(static_cast<unsigned char>(sql[pos]))) break;
		std::string word;
		while (pos < length && (isalnum(static_cast<unsigned char>(sql[pos])) || sql[pos] == '_'))
			word += static_cast<char>(toupper(static_cast<unsigned char>(sql[pos++])));
		words.push_back(std::move(word));
	}
	return words;
}

// StarRocks keeps an explicit transaction in the FE connection but does not
// report it in the OK packet status flags, so we follow the statements ourselves.
// Returns 1 if the text begins a transaction, -1 if it ends one and 0 otherwise.
// We fear that a COMMIT hidden in a literal of a multi-statement text would make
// us treat a transaction as finished while it is open, so a multi-statement
// text can only begin a transaction, never end one.
inline int starrocks_transaction_command(const char* sql, size_t length) {
	const auto classify = [](const char* text, size_t size) {
		const std::vector<std::string> words = starrocks_leading_words(text, size, 2);
		if (words.empty()) return 0;
		if (words[0] == "BEGIN" || (words[0] == "START" && words.size() == 2 && words[1] == "TRANSACTION")) return 1;
		if (words[0] == "COMMIT" || words[0] == "ROLLBACK") return -1;
		return 0;
	};
	const char* first_end = static_cast<const char*>(memchr(sql, ';', length));
	if (!first_end) return classify(sql, length);
	size_t rest = static_cast<size_t>(first_end - sql) + 1;
	while (rest < length && isspace(static_cast<unsigned char>(sql[rest]))) ++rest;
	if (rest == length) return classify(sql, static_cast<size_t>(first_end - sql));
	for (size_t pos = 0; pos < length;) {
		if (classify(sql + pos, length - pos) == 1) return 1;
		const char* separator = static_cast<const char*>(memchr(sql + pos, ';', length - pos));
		if (!separator) break;
		pos = static_cast<size_t>(separator - sql) + 1;
	}
	return 0;
}

#endif /* PROXYSQL_STARROCKS_TRANSACTION_H */
