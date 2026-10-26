import Config

# Codepoint counting keeps `max_length` a real bound on stored bytes.
config :ash, default_string_length_count: :codepoints
