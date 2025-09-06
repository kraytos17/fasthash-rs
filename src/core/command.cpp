// Copyright 2025 Soubhik Gon
#include <charconv>
#include <print>
#include <string_view>

#include "core/command.hpp"
#include "core/parser.hpp"

using namespace std::string_view_literals;

namespace fasthash {
    constexpr auto cmd_map = std::to_array<std::pair<std::string_view, Command::Type>>(
        {{"SET"sv, Command::Type::SET},
         {"GET"sv, Command::Type::GET},
         {"DEL"sv, Command::Type::DEL},
         {"EXPIRE"sv, Command::Type::EXPIRE},
         {"TTL"sv, Command::Type::TTL},
         {"SETEX"sv, Command::Type::SETEX},
         {"KEYS"sv, Command::Type::KEYS},
         {"EXISTS"sv, Command::Type::EXISTS},
         {"PERSIST"sv, Command::Type::PERSIST},
         {"FLUSHALL"sv, Command::Type::FLUSHALL},
         {"SAVE"sv, Command::Type::SAVE},
         {"ASAVE"sv, Command::Type::ASAVE},
         {"LOAD"sv, Command::Type::LOAD}});


    Command Command::parse(std::string_view line) {
        Command cmd{};
        cmd.args = parser::tokenize(line);
        if (cmd.args.empty()) {
            return cmd;
        }

        auto cmd_name = parser::to_upper(cmd.args[0]);
        for (const auto& [name, type]: cmd_map) {
            if (name == cmd_name) {
                cmd.type = type;
                return cmd;
            }
        }

        return cmd;
    }

    void Command::execute(FastHash& store) const {
        using enum Type;

        const auto print_error = [](const std::string_view usage) {
            std::println("ERROR: {}", usage);
        };
        switch (type) {
            case SET:
                if (args.size() != 3) {
                    print_error("SET usage: SET key value");
                    break;
                }

                store.set(args[1], args[2]);
                std::println("OK");
                break;
            case GET:
                if (args.size() != 2) {
                    print_error("GET usage: GET key");
                    break;
                }
                if (auto val = store.get(args[1]); val.has_value()) {
                    std::println("{}", val.value());
                } else {
                    std::println("(nil)");
                }
                break;
            case DEL:
                if (args.size() != 2) {
                    print_error("DEL usage: DEL key");
                    break;
                }

                std::println("{}", store.del(args[1]) ? "OK" : "(nil)");
                break;
            case EXPIRE: {
                if (args.size() != 3) {
                    print_error("EXPIRE usage: EXPIRE key seconds");
                    break;
                }

                int seconds;
                auto [ptr, ec] =
                    std::from_chars(args[2].data(), args[2].data() + args[2].size(), seconds);

                if (ec == std::errc()) {
                    std::println("{}", store.expire(args[1], seconds) ? "OK" : "(nil)");
                } else {
                    print_error("invalid seconds");
                }
                break;
            }
            case TTL: {
                if (args.size() != 2) {
                    print_error("TTL usage: TTL key");
                    break;
                }
                switch (int remaining = store.ttl(args[1])) {
                    case -2:
                        std::println("(nil)");
                        break;
                    case -1:
                        std::println("-1");
                        break;
                    default:
                        std::println("{}", remaining);
                        break;
                }
                break;
            }
            case SETEX: {
                if (args.size() != 4) {
                    print_error("SETEX usage: SETEX key seconds value");
                    break;
                }

                int seconds;
                auto [ptr, ec] =
                    std::from_chars(args[2].data(), args[2].data() + args[2].size(), seconds);

                if (ec == std::errc()) {
                    store.set(args[1], args[3], seconds);
                    std::println("OK");
                } else {
                    print_error("invalid seconds");
                }
                break;
            }
            case KEYS: {
                if (args.size() != 2) {
                    print_error("Usage: KEYS pattern");
                    break;
                }
                for (const auto& key: store.keys(args[1])) {
                    std::println("{}", key);
                }
                break;
            }
            case EXISTS:
                if (args.size() != 2) {
                    print_error("Usage: EXISTS key");
                    break;
                }

                std::println("{}", store.exists(args[1]) ? 1 : 0);
                break;
            case PERSIST:
                if (args.size() != 2) {
                    print_error("Usage: PERSIST key");
                    break;
                }

                std::println("{}", store.persist(args[1]) ? 1 : 0);
                break;
            case FLUSHALL:
                if (args.size() != 1) {
                    print_error("FLUSHALL takes no arguments");
                    break;
                }

                store.flush_all();
                std::println("OK");
                break;
            case SAVE:
                if (args.size() != 1) {
                    print_error("SAVE takes no arguments");
                    break;
                }

                std::println("{}", store.save() ? "OK" : "ERROR: Failed to save");
                break;
            case ASAVE:
                if (args.size() != 1) {
                    print_error("ASAVE takes no arguments");
                    break;
                }

                std::println("{}",
                             store.save_async() ? "OK" : "ERROR: Failed to save asynchronously");
                break;
            case LOAD:
                if (args.size() != 1) {
                    print_error("LOAD takes no arguments");
                    break;
                }

                std::println("{}", store.load() ? "OK" : "ERROR: Failed to load dumpfile");
                break;
            case INVALID:
            default:
                print_error("unknown command");
                break;
        }
    }
} // namespace fasthash
