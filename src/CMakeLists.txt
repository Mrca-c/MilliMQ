add_executable(millimq_server
    main_network.cpp
    segment_file.cpp
    segment_manager.cpp
    file_handle_pool.cpp
    message_codec.cpp
    index_manager.cpp
    network_server.cpp
    protocol.cpp
)

add_executable(stress_test
    main.cpp
    segment_file.cpp
    segment_manager.cpp
    file_handle_pool.cpp
    message_codec.cpp
    index_manager.cpp
)

add_executable(test_client
    ../tests/test_client_final.cpp
)

add_executable(bench_produce
    ../tests/bench_produce.cpp
)

add_executable(test_message_codec
    ../tests/test_message_codec.cpp
    message_codec.cpp
)

add_executable(test_protocol
    ../tests/test_protocol.cpp
    protocol.cpp
)

add_executable(test_file_handle_pool
    ../tests/test_file_handle_pool.cpp
    file_handle_pool.cpp
)

add_executable(test_segment_manager
    ../tests/test_segment_manager.cpp
    segment_manager.cpp
    segment_file.cpp
    file_handle_pool.cpp
)