#ifndef NEMO_INCLUDE_NEMO_OPTIONS_H_
#define NEMO_INCLUDE_NEMO_OPTIONS_H_

namespace nemo {

struct Options {
    bool create_if_missing;
    int write_buffer_size;
    int max_open_files;
    bool use_bloomfilter;
    int write_threads;

    // default target_file_size_base and multiplier is the save as rocksdb
    int target_file_size_base;
    int target_file_size_multiplier;
    bool compression;
    int max_background_flushes;
    int max_background_compactions;

	Options() : create_if_missing(true),
        write_buffer_size(4 * 1024 * 1024),
        max_open_files(5000),
        use_bloomfilter(true),
        write_threads(71),
        target_file_size_base(2 * 1024 * 1024),
        target_file_size_multiplier(1),
        compression(true),
        max_background_flushes(1),
        max_background_compactions(1) {}
};

}; // end namespace nemo

#endif
