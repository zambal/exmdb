defmodule Exmdb.Util do
  @moduledoc false

  @default_timeout 5_000

  def timeout(opts) do
    Keyword.get(opts, :timeout, @default_timeout)
  end

  def expand_db_spec(dbs, opts) do
    case Keyword.get(opts, :db) do
      nil ->
        if is_map(dbs) do
          raise "db name required"
        end
        expand_db_spec(dbs)
      name when is_binary(name) ->
        if is_map(dbs) do
          db = Map.get(dbs, name)
          if is_nil(db) do
            raise "named database could not be found"
          end
          expand_db_spec(db)
        else
          raise "named databases not supported"
        end
    end
  end

  defp expand_db_spec(db) do
    {dbi, spec} = db
    {
      dbi,
      Keyword.get(spec, :key_type, :ordered_term),
      Keyword.get(spec, :val_type, :term)
    }
  end

  def encode(data, :binary), do: data
  def encode(data, :term), do: :erlang.term_to_binary(data)
  def encode(data, :ordered_term), do: :sext.encode(data)

  def decode(data, :binary), do: data
  def decode(data, :term), do: :erlang.binary_to_term(data)
  def decode(data, :ordered_term), do: :sext.decode(data)
end
