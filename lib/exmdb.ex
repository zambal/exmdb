defmodule Exmdb do
  alias Exmdb.Env
  alias Exmdb.Txn

  @spec create(Path.t, Env.env_create_opts) :: {:ok, Env.t} | {:error, :exists}
  defdelegate create(path, opts \\ []), to: Env

  @spec open(Path.t, Env.env_opts) :: {:ok, Env.t} | {:error, :not_found}
  defdelegate open(path, opts \\ []), to: Env

  @spec close(Env.t) :: :ok
  defdelegate close(env), to: Env

  @default_timeout 5_000

  def put(env_or_txn, key, value, opts \\ [])
  def put(%Env{dbs: dbs} = env, key, value, opts) do
    {dbi, key_type, val_type} = expand_db_spec(dbs, opts)
    case :elmdb.async_put(dbi, encode(key, key_type), encode(value, val_type)) do
      :ok ->
        env
      {:error, {_code, msg}} ->
        raise List.to_string(msg)
    end
  end
  def put(%Txn{type: :rw, res: res, env: env}, key, value, opts) do
    {dbi, key_type, val_type} = expand_db_spec(env.dbs, opts)
    case :elmdb.txn_put(res, dbi, encode(key, key_type), encode(value, val_type), Keyword.get(opts, :timeout, @default_timeout)) do
      :ok ->
        env
      {:error, {_code, msg}} ->
        raise List.to_string(msg)
    end
  end

  def get(env_or_txn, key, default \\ nil, opts \\ [])
  def get(%Env{dbs: dbs}, key, default, opts) do
    {dbi, key_type, val_type} = expand_db_spec(dbs, opts)
    case :elmdb.get(dbi, encode(key, key_type)) do
      {:ok, val} ->
        decode(val, val_type)
      :not_found ->
        default
      {:error, {_code, msg}} ->
        raise List.to_string(msg)
    end
  end
  def get(%Txn{type: :rw, res: res, env: env}, key, default, opts) do
    {dbi, key_type, val_type} = expand_db_spec(env.dbs, opts)
    case :elmdb.txn_get(res, dbi, encode(key, key_type), Keyword.get(opts, :timeout, @default_timeout)) do
      {:ok, val} ->
        decode(val, val_type)
      :not_found ->
        default
      {:error, {_code, msg}} ->
        raise List.to_string(msg)
    end
  end

  defp transaction(%Env{res: res} = env, fun, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    txn_type = Keyword.get(opts, :type, :rw)
    case txn_begin(res, timeout, txn_type) do
      {:ok, txn_res} ->
        txn = %Txn{res: txn_res, env: env, type: txn_type}
        try do
          fun.(txn)
        rescue
          error ->
            st = System.stacktrace()
            txn_abort(txn_res, timeout, txn_type)
            reraise(error, st)
        catch
          :exit, reason ->
            txn_abort(txn_res, timeout, txn_type)
            exit(reason)
          :throw, :txn_abort ->
            txn_abort(txn_res, timeout, txn_type)
            :aborted
          :throw, reason ->
            txn_abort(txn_res, timeout, txn_type)
            throw(reason)
        else result ->
          case txn_commit(txn_res, timeout, txn_type) do
            :ok ->
              {:ok, result}
            {:error, {_code, msg}} ->
              raise msg
          end
        end
      {:error, {_code, msg}} ->
        raise msg
    end
  end

  def abort do
    throw :txn_abort
  end

  defp txn_begin(txn_res, timeout, :rw) do
    :elmdb.txn_begin(txn_res, timeout)
  end
  defp txn_begin(txn_res, timeout, :ro) do
    :elmdb.ro_txn_begin(txn_res, timeout)
  end

  defp txn_commit(txn_res, timeout, :rw) do
    :elmdb.txn_commit(txn_res, timeout)
  end
  defp txn_commit(txn_res, timeout, :ro) do
    :elmdb.ro_txn_commit(txn_res, timeout)
  end

  defp txn_abort(txn_res, timeout, :rw) do
    :elmdb.txn_abort(txn_res, timeout)
  end
  defp txn_abort(txn_res, timeout, :ro) do
    :elmdb.ro_txn_abort(txn_res, timeout)
  end

  defp expand_db_spec(dbs, opts) do
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

  defp encode(data, :binary), do: data
  defp encode(data, :term), do: :erlang.term_to_binary(data)
  defp encode(data, :ordered_term), do: :sext.encode(data)

  defp decode(data, :binary), do: data
  defp decode(data, :term), do: :erlang.binary_to_term(data)
  defp decode(data, :ordered_term), do: :sext.decode(data)
end
