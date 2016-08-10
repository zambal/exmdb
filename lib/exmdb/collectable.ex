defmodule Exmdb.Collectable do
  import Exmdb.Util

  defstruct src: nil, db_spec: nil, timeout: 5_000

  @type t :: %Exmdb.Collectable{
    src: Exmdb.source,
    db_spec: Exmdb.Env.db_spec,
  }

  def new(env_or_txn, opts) do
    %Exmdb.Collectable{
      src: env_or_txn,
      db_spec: get_db_spec(env_or_txn, opts),
      timeout: timeout(opts)
    }
  end

  defp get_db_spec(%Exmdb.Env{dbs: dbs}, opts), do: db_spec(dbs, opts)
  defp get_db_spec(%Exmdb.Txn{env: env}, opts), do: db_spec(env.dbs, opts)
end

defimpl Collectable, for: Exmdb.Collectable do
  import Exmdb.Util

  def into(original) do
    {original, fn
      %Exmdb.Collectable{src: src, db_spec: db_spec, timeout: timeout} = col, {:cont, {k, v}} ->
        :ok = put(src, k, v, db_spec, timeout)
        col
      col, :done ->
        col.src
      _, :halt ->
        :ok
    end}
  end

  defp put(%Exmdb.Env{}, key, value, {dbi, key_type, val_type}, timeout) do
    case :elmdb.async_put(dbi, encode(key, key_type), encode(value, val_type), timeout) do
      :ok         -> :ok
      {:error, e} -> mdb_error(e)
    end
  end
  defp put(%Exmdb.Txn{type: :rw, res: res}, key, value, {dbi, key_type, val_type}, timeout) do
    case :elmdb.txn_put(res, dbi, encode(key, key_type), encode(value, val_type), timeout) do
      :ok         -> :ok
      {:error, e} -> mdb_error(e)
    end
  end
  defp put(%Exmdb.Txn{type: :ro}, _key, _value, _db_spec, _timeout) do
    raise ArgumentError, message: "Exmdb.Collectable can only be used within a read/write transaction"
  end
end
