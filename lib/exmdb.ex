defmodule Exmdb do
  alias Exmdb.{Env, Range, Txn}
  import Exmdb.Util

  @type source :: Env.t | Txn.t
  @type query_opts :: [{:timeout, non_neg_integer} | {:db, binary}]

  @spec create(Path.t, Env.env_create_opts) :: {:ok, Env.t} | {:error, :exists}
  defdelegate create(path, opts \\ []), to: Env

  @spec open(Path.t, Env.env_opts) :: {:ok, Env.t} | {:error, :not_found}
  defdelegate open(path, opts \\ []), to: Env

  @spec close(Env.t) :: :ok
  defdelegate close(env), to: Env

  @spec put(source, any, any, query_opts) :: source
  def put(env_or_txn, key, value, opts \\ [])
  def put(%Env{dbs: dbs} = env, key, value, opts) do
    {dbi, key_type, val_type} = db_spec(dbs, opts)
    case :elmdb.async_put(dbi, encode(key, key_type), encode(value, val_type), timeout(opts)) do
      :ok         -> env
      {:error, e} -> mdb_error(e)
    end
  end
  def put(%Txn{type: :rw, res: res, env: env} = txn, key, value, opts) do
    {dbi, key_type, val_type} = db_spec(env.dbs, opts)
    case :elmdb.txn_put(res, dbi, encode(key, key_type), encode(value, val_type), timeout(opts)) do
      :ok         -> txn
      {:error, e} -> mdb_error(e)
    end
  end

  def delete(%Env{dbs: dbs} = env, key, opts \\ []) do
    {dbi, key_type, val_type} = db_spec(dbs, opts)
    case :elmdb.async_delete(dbi, encode(key, key_type), timeout(opts)) do
      :ok         -> env
      {:error, e} -> mdb_error(e)
    end
  end

  def set_comparator(%Txn{type: :rw, res: res, env: env} = txn) do
    {dbi, _, _} = db_spec(env.dbs, [])
    :elmdb.set_comparator(res, dbi)
  end

  @spec get(source, any, any, query_opts) :: any
  def get(env_or_txn, key, default \\ nil, opts \\ [])
  def get(%Env{dbs: dbs}, key, default, opts) do
    {dbi, key_type, val_type} = db_spec(dbs, opts)
    case :elmdb.get(dbi, encode(key, key_type)) do
      {:ok, val}  -> decode(val, val_type)
      :not_found  -> default
      {:error, e} -> mdb_error(e)
    end
  end
  def get(%Txn{type: :rw, res: res, env: env}, key, default, opts) do
    {dbi, key_type, val_type} = db_spec(env.dbs, opts)
    case :elmdb.txn_get(res, dbi, encode(key, key_type), timeout(opts)) do
      {:ok, val}  -> decode(val, val_type)
      :not_found  -> default
      {:error, e} -> mdb_error(e)
    end
  end

  @spec transaction(Env.t, (Txn.t -> any)) :: {:ok, any} | :aborted
  def transaction(%Env{res: res} = env, fun, opts \\ []) do
    timeout = timeout(opts)
    txn_type = Keyword.get(opts, :type, :rw)
    case txn_begin(res, timeout, txn_type) do
      {:ok, txn_res} ->
        txn = %Txn{res: txn_res, env: env, type: txn_type}
        try do
          fun.(txn)
        rescue
          error ->
            st = System.stacktrace()
            _ = txn_abort(txn_res, timeout, txn_type)
            reraise(error, st)
        catch
          :exit, reason ->
            _ = txn_abort(txn_res, timeout, txn_type)
            exit(reason)
          :throw, :txn_abort ->
            _ = txn_abort(txn_res, timeout, txn_type)
            :aborted
          :throw, reason ->
            _ = txn_abort(txn_res, timeout, txn_type)
            throw(reason)
        else result ->
          case txn_commit(txn_res, timeout, txn_type) do
            :ok         -> {:ok, result}
            {:error, e} -> mdb_error(e)
          end
        end
      {:error, e} ->
        mdb_error(e)
    end
  end

  @spec abort() :: no_return
  def abort do
    throw :txn_abort
  end

  @spec range(source, Range.opts) :: Range.t
  defdelegate range(env_or_txn, opts \\ []), to: Range, as: :new

  @spec collect(source, query_opts) :: Exmdb.Collectable.t
  defdelegate collect(env_or_txn, opts \\ []), to: Exmdb.Collectable, as: :new

  defp txn_begin(txn_res, timeout, :rw) do
    :elmdb.txn_begin(txn_res, timeout)
  end
  defp txn_begin(txn_res, _timeout, :ro) do
    :elmdb.ro_txn_begin(txn_res)
  end

  defp txn_commit(txn_res, timeout, :rw) do
    :elmdb.txn_commit(txn_res, timeout)
  end
  defp txn_commit(txn_res, _timeout, :ro) do
    :elmdb.ro_txn_commit(txn_res)
  end

  defp txn_abort(txn_res, timeout, :rw) do
    :elmdb.txn_abort(txn_res, timeout)
  end
  defp txn_abort(txn_res, _timeout, :ro) do
    :elmdb.ro_txn_abort(txn_res)
  end
end
