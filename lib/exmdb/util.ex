defmodule Exmdb.Util do
  @moduledoc false

  def db_spec(dbs, opts) do
    case Keyword.get(opts, :db) do
      nil ->
        if is_map(dbs), do: raise("db name required")
        dbs

      name ->
        if is_map(dbs) do
          db_spec = Map.get(dbs, name)
          if is_nil(db_spec), do: raise("named database #{inspect(name)} could not be found")
          db_spec
        else
          raise "named databases not supported"
        end
    end
  end

  def encode(data, :binary), do: data
  def encode(data, :term), do: :erlang.term_to_binary(data)
  def encode(data, :ordered_term), do: :sext.encode(data)

  def decode(data, :binary), do: data
  def decode(data, :term), do: :erlang.binary_to_term(data)
  def decode(data, :ordered_term), do: :sext.decode(data)

  defmacro timeout(opts) do
    quote do
      Keyword.get(unquote(opts), :timeout, 5_000)
    end
  end

  defmacro mdb_error(e) do
    quote do
      {_code, msg} = unquote(e)
      raise List.to_string(msg)
    end
  end
end
