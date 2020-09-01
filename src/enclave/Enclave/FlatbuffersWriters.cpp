#include "FlatbuffersWriters.h"
#include "EnclaveContext.h"
#include <iostream>
#include <iomanip>

void RowWriter::clear() {
  builder.Clear();
  rows_vector.clear();
  total_num_rows = 0;
  enc_block_builder.Clear();
  enc_block_vector.clear();
  finished = false;
}

void RowWriter::append(const tuix::Row *row) {
  rows_vector.push_back(flatbuffers_copy(row, builder));
  total_num_rows++;
  maybe_finish_block();
}

void RowWriter::append(const std::vector<const tuix::Field *> &row_fields) {
  flatbuffers::uoffset_t num_fields = row_fields.size();
  std::vector<flatbuffers::Offset<tuix::Field>> field_values(num_fields);
  for (flatbuffers::uoffset_t i = 0; i < num_fields; i++) {
    field_values[i] = flatbuffers_copy<tuix::Field>(row_fields[i], builder);
  }
  rows_vector.push_back(tuix::CreateRowDirect(builder, &field_values));
  total_num_rows++;
  maybe_finish_block();
}

void RowWriter::append(const tuix::Row *row1, const tuix::Row *row2) {
  flatbuffers::uoffset_t num_fields = row1->field_values()->size() + row2->field_values()->size();
  std::vector<flatbuffers::Offset<tuix::Field>> field_values(num_fields);
  flatbuffers::uoffset_t i = 0;
  for (auto it = row1->field_values()->begin(); it != row1->field_values()->end(); ++it, ++i) {
    field_values[i] = flatbuffers_copy<tuix::Field>(*it, builder);
  }
  for (auto it = row2->field_values()->begin(); it != row2->field_values()->end(); ++it, ++i) {
    field_values[i] = flatbuffers_copy<tuix::Field>(*it, builder);
  }
  rows_vector.push_back(tuix::CreateRowDirect(builder, &field_values));
  total_num_rows++;
  maybe_finish_block();
}

UntrustedBufferRef<tuix::EncryptedBlocks> RowWriter::output_buffer(std::string ecall) {
  // std::cout << "Finished? " << finished << std::endl;
  if (!finished) { // This line causes ExternalSort not to call finish_blocks() in first output_buffer()
    finish_blocks(ecall);
  }

  // Allocate enc block builder's buffer size outside enclave
  uint8_t *buf_ptr;
  ocall_malloc(enc_block_builder.GetSize(), &buf_ptr);

  // Copy the buffer to untrusted memory
  std::unique_ptr<uint8_t, decltype(&ocall_free)> buf(buf_ptr, &ocall_free);
  memcpy(buf.get(), enc_block_builder.GetBufferPointer(), enc_block_builder.GetSize());

  // Create an UntrustedBufferRef out of the untrusted memory
  UntrustedBufferRef<tuix::EncryptedBlocks> buffer(
    std::move(buf), enc_block_builder.GetSize());

  return buffer;
}

void RowWriter::output_buffer(uint8_t **output_rows, size_t *output_rows_length, std::string ecall) {
  // Get the UntrustedBufferRef
  auto result = output_buffer(ecall);

  // output rows is a reference to encrypted blocks in untrusted memory
  *output_rows = result.buf.release();
  *output_rows_length = result.len;

}

uint32_t RowWriter::num_rows() {
  return total_num_rows;
}


void RowWriter::maybe_finish_block() {
  if (builder.GetSize() >= MAX_BLOCK_SIZE) {
    finish_block();
  }
}

void RowWriter::finish_block() {
  // Serialize the rows
  builder.Finish(tuix::CreateRowsDirect(builder, &rows_vector));
  size_t enc_rows_len = enc_size(builder.GetSize());

  // Allocate space for block in untrusted memory
  uint8_t *enc_rows_ptr = nullptr;
  ocall_malloc(enc_rows_len, &enc_rows_ptr);

  // Encrypt the serialized rows and push the ciphertext to untrusted memory
  std::unique_ptr<uint8_t, decltype(&ocall_free)> enc_rows(enc_rows_ptr, &ocall_free);
  // TODO: create a temporary buffer that stores serialized rows inside enclave, then copy these rows to enc_rows.get() to retrieve MAC
  encrypt(builder.GetBufferPointer(), builder.GetSize(), enc_rows.get());

  // Add each EncryptedBlock's MAC to the log entry so that next partition can check it
  // we only want to add the mac if it's not part of the join primary group reader
  if (EnclaveContext::getInstance().to_append_mac()) {
    uint8_t mac[SGX_AESGCM_MAC_SIZE];
    memcpy(mac, enc_rows.get() + SGX_AESGCM_IV_SIZE + builder.GetSize(), SGX_AESGCM_MAC_SIZE);
    EnclaveContext::getInstance().add_mac_to_mac_lst(mac);
  }

  // Add the offset to enc_block_vector
  enc_block_vector.push_back(
      // Create offset into enc_block_builder where the entire EncryptedBlock is
    tuix::CreateEncryptedBlock(
      enc_block_builder,
      rows_vector.size(),
      // Create offset into enc_block_builder to find serialized rows
      enc_block_builder.CreateVector(enc_rows.get(), enc_rows_len)));

  // Clear the entire row FlatBufferBuilder
  builder.Clear();
  rows_vector.clear();
}

flatbuffers::Offset<tuix::EncryptedBlocks> RowWriter::finish_blocks(std::string curr_ecall) {
  if (rows_vector.size() > 0) {
    finish_block();
  }

  std::vector<flatbuffers::Offset<tuix::LogEntry>> curr_log_entry_vector;
  std::vector<flatbuffers::Offset<tuix::LogEntry>> past_log_entries_vector;
  std::vector<int> num_past_log_entries;
  std::vector<uint8_t> log_hash;
  std::vector<flatbuffers::Offset<tuix::LogEntryChainHash>> log_entry_chain_hash_vector;
  

  if (curr_ecall != std::string("NULL")) {
    // Only write log entry chain if this is the output of an ecall, e.g. not primary group in SortMergeJoin
    int job_id = EnclaveContext::getInstance().get_job_id();
    int num_macs = static_cast<int>(EnclaveContext::getInstance().get_num_macs());
    // std::cout << "Num macs: " << num_macs << std::endl;
    uint8_t mac_lst[num_macs * SGX_AESGCM_MAC_SIZE];
    uint8_t global_mac[OE_HMAC_SIZE];
    EnclaveContext::getInstance().hmac_mac_lst(mac_lst, global_mac);

    int curr_pid = EnclaveContext::getInstance().get_pid();
    // uint8_t* global_mac = EnclaveContext::getInstance().get_global_mac();
    char* untrusted_curr_ecall_str = oe_host_strndup(curr_ecall.c_str(), curr_ecall.length());

    // Copy mac list to untrusted memory
    uint8_t* untrusted_mac_lst = nullptr;
    ocall_malloc(num_macs * SGX_AESGCM_MAC_SIZE, &untrusted_mac_lst);
    std::unique_ptr<uint8_t, decltype(&ocall_free)> mac_lst_ptr(untrusted_mac_lst, &ocall_free);
    memcpy(mac_lst_ptr.get(), mac_lst, num_macs * SGX_AESGCM_MAC_SIZE);

    // Copy global mac to untrusted memory
    uint8_t* untrusted_global_mac = nullptr;
    ocall_malloc(OE_HMAC_SIZE, &untrusted_global_mac);
    std::unique_ptr<uint8_t, decltype(&ocall_free)> global_mac_ptr(untrusted_global_mac, &ocall_free);
    memcpy(global_mac_ptr.get(), global_mac, OE_HMAC_SIZE);

    // This is an offset into enc block builder
    auto log_entry_serialized = tuix::CreateLogEntry(enc_block_builder,
        enc_block_builder.CreateString(std::string(untrusted_curr_ecall_str)),
        curr_pid,
        -1, // -1 for not yet set rcv_pid
        job_id,
        num_macs,
        enc_block_builder.CreateVector(mac_lst_ptr.get(), num_macs * SGX_AESGCM_MAC_SIZE),
        enc_block_builder.CreateVector(global_mac_ptr.get(), OE_HMAC_SIZE));

    curr_log_entry_vector.push_back(log_entry_serialized);

    std::vector<LogEntry> past_log_entries = EnclaveContext::getInstance().get_ecall_log_entries();

    for (LogEntry le : past_log_entries) {
      char* untrusted_ecall_op_str = oe_host_strndup(le.op.c_str(), le.op.length());
      auto past_log_entry_serialized = tuix::CreateLogEntry(enc_block_builder,
          enc_block_builder.CreateString(std::string(untrusted_ecall_op_str)),
          le.snd_pid,
          le.rcv_pid,
          le.job_id);
      past_log_entries_vector.push_back(past_log_entry_serialized);
    }

    num_past_log_entries.push_back(past_log_entries.size());
   
    // We will hash over global_mac || curr_ecall || snd_pid || rcv_pid || job_id || num_macs || past log entries
    // 
    int past_ecalls_lengths = 0;
    for (size_t i = 0; i < past_log_entries.size(); i++) {
      auto past_log_entry = past_log_entries[i];
      std::string ecall = past_log_entry.op;
      past_ecalls_lengths += ecall.length();
    }
    int past_entries_num_bytes_minus_ecall_lengths = 3 * sizeof(int);

    int num_bytes_to_hash = OE_HMAC_SIZE + 4 * sizeof(int) + curr_ecall.length() + past_entries_num_bytes_minus_ecall_lengths * past_log_entries.size() + past_ecalls_lengths;
    // int num_bytes_to_hash = OE_HMAC_SIZE + 4 * sizeof(int) + curr_ecall.length() + sizeof(LogEntry) * past_log_entries.size();
    std::cout << "Hashing this many bytes " << num_bytes_to_hash << std::endl;
    // int num_bytes_to_hash = OE_HMAC_SIZE + 3 * sizeof(int) + sizeof(size_t) + curr_ecall.length() + 1 + past_entries_num_bytes_minus_ecall_lengths * past_log_entries.size() + past_ecalls_lengths;
    uint8_t to_hash[num_bytes_to_hash];
    int rcv_pid = -1;

    memcpy(to_hash, global_mac, OE_HMAC_SIZE);
    memcpy(to_hash + OE_HMAC_SIZE, curr_ecall.c_str(), curr_ecall.length());
    memcpy(to_hash + OE_HMAC_SIZE + curr_ecall.length(), &curr_pid, sizeof(int));
    memcpy(to_hash + OE_HMAC_SIZE + curr_ecall.length() + sizeof(int), &rcv_pid, sizeof(int));
    memcpy(to_hash + OE_HMAC_SIZE + curr_ecall.length() + 2 * sizeof(int), &job_id, sizeof(int));
    memcpy(to_hash + OE_HMAC_SIZE + curr_ecall.length() + 3 * sizeof(int), &num_macs, sizeof(int));
    // memcpy(to_hash + OE_HMAC_SIZE + curr_ecall.length() + 4 * sizeof(int), past_log_entries.data(), past_log_entries.size() * sizeof(LogEntry));

    // std::cout << "Sent global hmac: =========" << std::endl;
    // for (int k = 0; k < OE_HMAC_SIZE; k++) {
    //   std::cout << (int) global_mac[k] << " ";
    // }
    // std::cout << std::endl;
    // std::cout << curr_ecall.c_str() << std::endl;
    // std::cout << "curr pid: " << curr_pid << std::endl;
    // std::cout << "job id: " << job_id << std::endl;
    // std::cout << "num macs: " << num_macs << std::endl;

    // // Copy over data from past log entries
    uint8_t* tmp_ptr = to_hash + OE_HMAC_SIZE + curr_ecall.length() + 4 * sizeof(int);
    for (size_t i = 0; i < past_log_entries.size(); i++) {
      auto past_log_entry = past_log_entries[i];
      std::string ecall = past_log_entry.op;
      int pe_snd_pid = past_log_entry.snd_pid;
      int pe_rcv_pid = past_log_entry.rcv_pid;
      int pe_job_id = past_log_entry.job_id;
      
      // std::cout << "snd pid: " << pe_snd_pid << " || pe_rcv_pid: " << pe_rcv_pid << " || pe_job_id: " << pe_job_id << std::endl;

      int bytes_copied = ecall.length() + 3 * sizeof(int);
    
      memcpy(tmp_ptr, ecall.c_str(), ecall.length());
      memcpy(tmp_ptr + ecall.length(), &pe_snd_pid, sizeof(int));
      memcpy(tmp_ptr + ecall.length() + sizeof(int), &pe_rcv_pid, sizeof(int));
      memcpy(tmp_ptr + ecall.length() + 2 * sizeof(int), &pe_job_id, sizeof(int));

      tmp_ptr += bytes_copied;
    }
    // std::cout << "Expect " << past_log_entries.size() << " past log entries\n";
// 
    // std::cout << "Writers: Output of to hash\n";
    // for (int j = 0; j < num_bytes_to_hash; j++) {
      // std::cout << int(to_hash[j]) << " ";
    // }
    // std::cout << std::endl;
    // Hash the data
    uint8_t hash[32];
    mcrypto.sha256(to_hash, num_bytes_to_hash, hash);

    // Print the hash for debugging purposes
    // for (int j = 0; j < 32; j++) {
    //   std::cout << int(hash[j]) << " ";
    // }
    // std::cout << std::endl;

    // Copy the hash to untrusted memory
    uint8_t* untrusted_hash = nullptr;
    ocall_malloc(32, &untrusted_hash);
    std::unique_ptr<uint8_t, decltype(&ocall_free)> hash_ptr(untrusted_hash, &ocall_free);
    memcpy(hash_ptr.get(), hash, 32);
    // log_hash(hash_ptr.get(), hash_ptr.get() + 32);
    // memcpy(untrusted_hash, hash, 32);
    // log_hash(untrusted_hash, untrusted_hash + 32);
    auto hash_offset = tuix::CreateLogEntryChainHash(enc_block_builder, enc_block_builder.CreateVector(hash_ptr.get(), 32));
    log_entry_chain_hash_vector.push_back(hash_offset);

    // Clear log entry state
    EnclaveContext::getInstance().reset_log_entry();
  } 

  auto log_entry_chain_serialized = tuix::CreateLogEntryChainDirect(enc_block_builder, &curr_log_entry_vector, &past_log_entries_vector, &num_past_log_entries);
  auto result = tuix::CreateEncryptedBlocksDirect(enc_block_builder, &enc_block_vector, log_entry_chain_serialized, &log_entry_chain_hash_vector);
  // auto result = tuix::CreateEncryptedBlocks(enc_block_builder, enc_block_builder.CreateVector(enc_block_vector, enc_block_vector.size()), log_entry_chain_serialized, enc_block_builder.CreateVector(hash_ptr.get(), 32));
  enc_block_builder.Finish(result);
  enc_block_vector.clear();

  finished = true;

  return result;
}

void SortedRunsWriter::clear() {
  container.clear();
  runs.clear();
}

void SortedRunsWriter::append(const tuix::Row *row) {
  container.append(row);
}

void SortedRunsWriter::append(const std::vector<const tuix::Field *> &row_fields) {
  container.append(row_fields);
}

void SortedRunsWriter::append(const tuix::Row *row1, const tuix::Row *row2) {
  container.append(row1, row2);
}

void SortedRunsWriter::finish_run(std::string ecall) {
  runs.push_back(container.finish_blocks(ecall));
}

uint32_t SortedRunsWriter::num_runs() {
  return runs.size();
}

UntrustedBufferRef<tuix::SortedRuns> SortedRunsWriter::output_buffer() {
  container.enc_block_builder.Finish(
    tuix::CreateSortedRunsDirect(container.enc_block_builder, &runs));

  uint8_t *buf_ptr;
  ocall_malloc(container.enc_block_builder.GetSize(), &buf_ptr);

  std::unique_ptr<uint8_t, decltype(&ocall_free)> buf(buf_ptr, &ocall_free);
  memcpy(buf.get(),
         container.enc_block_builder.GetBufferPointer(),
         container.enc_block_builder.GetSize());

  UntrustedBufferRef<tuix::SortedRuns> buffer(
    std::move(buf), container.enc_block_builder.GetSize());

  return buffer;
}

RowWriter *SortedRunsWriter::as_row_writer() {
  if (runs.size() > 1) {
    throw std::runtime_error("Invalid attempt to convert SortedRunsWriter with more than one run "
                             "to RowWriter");
  }

  return &container;
}
