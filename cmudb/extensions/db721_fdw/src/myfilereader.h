#pragma once

#include <fstream>

namespace myutil {

class FileReader {
 public:
  FileReader();

  virtual ~FileReader();

  auto Open(const std::string &file_path) -> bool;

  auto HasOpen() -> bool;

  auto HasEnd() -> bool;

  auto Seek(std::streamoff offset, std::ios_base::seekdir way) -> FileReader&;

  auto GetLength() -> size_t;

  auto SetLength() -> void;

  auto Tell() -> size_t;

  auto Close() -> void;

  auto Read4Bytes(char* dst) -> void {
    fin_.read(dst, 4);
  }

  auto Read4BytesBE(char* dst) -> void {
    this->fin_.read(dst + 3, 1);
    this->fin_.read(dst + 2, 1);
    this->fin_.read(dst + 1, 1);
    this->fin_.read(dst, 1);
  }

  auto Read1Bytes(char* dst) -> void {
    this->fin_.read(dst, 1);
  }

  auto ReadUInt8() -> uint8_t {
    uint8_t data;
    Read1Bytes(reinterpret_cast<char*>(&data));
    return data;
  }

  auto ReadUInt32() -> uint32_t {
    uint32_t data;
    Read4Bytes(reinterpret_cast<char*>(&data));
    return data;
  }

  auto ReadUInt32BE() -> uint32_t {
    uint32_t data;
    Read4BytesBE(reinterpret_cast<char*>(&data));
    return data;
  }

  auto ReadUInt8Array(size_t count) {
    uint8_t* data = new uint8_t[count];
    for (size_t x = 0; x < count; x++) {
      data[x] = ReadUInt8();
    }
    return data;
  }

  auto ReadAsciiString(size_t len) -> std::string {
    char* buffer = (char*)ReadUInt8Array(len);
    std::string data = std::string(buffer, len);
    delete[] buffer;
    return data;
  }

 private:
  std::string file_path_;
  std::ifstream fin_;
  bool has_open_;
  bool has_end_;
  size_t length_;
};

FileReader::FileReader() {
  file_path_ = "";
  has_open_ = false;
  has_end_ = false;
}

void FileReader::Close() {
  if (has_open_) {
    fin_.close();
  }
}

FileReader::~FileReader() {
  if (has_open_) {
    fin_.close();
  }
}

auto FileReader::Open(const std::string &directory) -> bool {
  if (fin_.is_open()) {
    fin_.close();
  }

  file_path_ = directory;
  fin_.open(directory, std::ifstream::in | std::ifstream::binary);
  
  if (fin_.fail()) {
    throw std::runtime_error("File does not exist");
  }

  this->SetLength();
  this->Seek(0, std::ios_base::beg);

  has_open_ = fin_.is_open();
  return has_open_;
}

auto FileReader::Seek(std::streamoff offset, std::ios_base::seekdir way) -> FileReader& {
    fin_.seekg(offset, way);
    return *this;
}

auto FileReader::SetLength() -> void {
  this->Seek(0, std::ios_base::end);
  this->length_ = this->Tell();
}

auto FileReader::GetLength() -> size_t {
  return length_;
}

auto FileReader::Tell() -> size_t {
  return (size_t)this->fin_.tellg();
}

auto FileReader::HasOpen() -> bool { return has_open_; }

auto FileReader::HasEnd() -> bool { return has_end_; }

} // end mytuil namespace
