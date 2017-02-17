#ifndef RPQ_PARSER_H_
#define RPQ_PARSER_H_

#include <cwctype>
#include <cctype>
#include <vector>
#include <exception>
#include <string>
#include <sstream>
#include <algorithm>
#include <functional>
#include <cctype>
#include <locale>

class RPQParseException : public std::exception {
  virtual const char* what() const throw () {
    return std::string("Error parsing regular expression.").c_str();
  }
};

struct RPQToken {
  RPQToken(const int i, const std::string& val)
      : id(i),
        value(val) {
  }

  RPQToken& RPQToken::operator=(const RPQToken& other) {
    id = other.id;
    value = other.value;
    return *this;
  }

  int id;
  std::string value;
};

class RPQLexer {
 public:
  static const int INVALID = -2;
  static const int END = -1;

  static const int LEFT = 0;
  static const int RIGHT = 1;
  static const int DOT = 2;
  static const int PLUS = 3;
  static const int LABEL = 4;

  RPQLexer() {
  }

  RPQLexer(const std::string& exp) {
    str(exp);
  }

  void str(const std::string& exp) {
    stream_.str(exp);
  }

  const RPQToken next() {
    while (iswspace(stream_.peek()))
      stream_.get();

    char c = stream_.get();
    switch (c) {
      case EOF:
        return RPQToken(END, "");
      case '(':
        return RPQToken(LEFT, "(");
      case ')':
        return RPQToken(RIGHT, ")");
      case '.':
        return RPQToken(DOT, ".");
      case '+':
        return RPQToken(PLUS, "+");
      default: {
        if (!isdigit(c))
          throw new RPQParseException;

        std::string label = "";
        while (isdigit(stream_.peek()))
          label += (char) stream_.get();

        if (stream_.peek() == '-') {
          label = "-" + label;
          stream_.get();
        }
        return RPQToken(LABEL, label);
      }
    }
  }

  const RPQToken peek() {
    const RPQToken tok = next();
    put_back(tok);
    return tok;
  }

  void put_back(const RPQToken& tok) {
    for (size_t i = 0; i < tok.value.size(); i++) {
      stream_.unget();
    }
  }

 private:
  std::stringstream stream_;
};

class RPQParser {
 public:
  RPQParser(std::string& exp) {
    rtrim(exp);
    if (exp.back() == '*') {
      recurse_ = true;
      exp.back() = ' ';
    }
    lex_.str(exp);
  }

  RPQuery parse() {
    RPQuery q;
    path_union_b(q.path_queries);
    q.recurse = recurse_;
    return q;
  }

 private:
  static inline void rtrim(std::string &s) {
    s.erase(
        std::find_if(s.rbegin(), s.rend(),
                     std::not1(std::ptr_fun<int, int>(std::isspace))).base(),
        s.end());
  }

  void path_union_b(std::vector<std::vector<int64_t>>& uq) {
    RPQToken tok = lex_.next();
    if (tok.id == RPQLexer::LEFT) {
      path_union(uq);
      tok = lex_.next();
      if (tok.id != RPQLexer::RIGHT)
        throw new RPQParseException;
    } else {
      lex_.put_back(tok);
      path_union_b(uq);
    }
  }

  void path_union(std::vector<std::vector<int64_t>>& uq) {
    std::vector<int64_t> pq;
    path_query_b(pq);
    uq.push_back(pq);

    while (true) {
      RPQToken tok = lex_.next();
      if (tok.id != RPQLexer::PLUS) {
        lex_.put_back(tok);
        break;
      }
      std::vector<int64_t> pql;
      path_query_b(pql);
      uq.push_back(pql);
    }
  }

  void path_query_b(std::vector<int64_t>& pq) {
    RPQToken tok = lex_.next();
    if (tok.id == RPQLexer::LEFT) {
      path_query(pq);
      tok = lex_.next();
      if (tok.id != RPQLexer::RIGHT)
        throw new RPQParseException;
    } else {
      lex_.put_back(tok);
      path_query(pq);
    }
  }

  void path_query(std::vector<int64_t>& pq) {
    RPQToken tok = lex_.next();
    if (tok.id != RPQLexer::LABEL)
      throw new RPQParseException;

    pq.push_back(std::stoll(tok.value));
    while (true) {
      tok = lex_.next();
      if (tok.id != RPQLexer::DOT) {
        lex_.put_back(tok);
        break;
      }
      tok = lex_.next();
      if (tok.id != RPQLexer::LABEL)
        throw new RPQParseException;
      pq.push_back(std::stoll(tok.value));
    }
  }

  RPQLexer lex_;
  bool recurse_;
};

#endif
