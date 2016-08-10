defmodule Exmdb.Collectable do
  import Exmdb.Util

  defstruct src: nil, db_spec: nil, timeout: 5_000

  @type t :: %Exmdb.Collectable{
    src: Exmdb.source,
    db_spec: Exmdb.Env.db_spec,
    timeout: non_neg_integer
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
  alias Exmdb.{Env, Txn}

  def into(original) do
    {original, fn
      col, {:cont, {k, v}} -> put(col, k, v)
      col, :done           -> col.src
      _col, :halt          -> :ok
    end}
  end

  defp put(%{src: %Env{}, db_spec: db_spec, timeout: timeout} = col, key, value) do
    {dbi, key_type, val_type} = db_spec
    case :elmdb.async_put(dbi, encode(key, key_type), encode(value, val_type), timeout) do
      :ok         -> col
      {:error, e} -> mdb_error(e)
    end
  end
  defp put(%{src: %Txn{type: :rw, res: res}, db_spec: db_spec, timeout: timeout} = col, key, value) do
    {dbi, key_type, val_type} = db_spec
    case :elmdb.txn_put(res, dbi, encode(key, key_type), encode(value, val_type), timeout) do
      :ok         -> col
      {:error, e} -> mdb_error(e)
    end
  end
  defp put(%{src: %Txn{type: :ro}}, _key, _value) do
    raise ArgumentError, message: "Exmdb.Collectable can only be used within a read/write transaction"
  end
end
