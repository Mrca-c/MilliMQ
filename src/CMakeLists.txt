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