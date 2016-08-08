defmodule Exmdb do
  alias Exmdb.{Env, Txn, Range}

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

  def transaction(%Env{res: res} = env, fun, opts \\ []) do
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

  defdelegate range(env, opts \\ []), to: Range, as: :new

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

  @doc false
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

  @doc false
  def encode(data, :binary), do: data
  def encode(data, :term), do: :erlang.term_to_binary(data)
  def encode(data, :ordered_term), do: :sext.encode(data)

  @doc false
  def decode(data, :binary), do: data
  def decode(data, :term), do: :erlang.binary_to_term(data)
  def decode(data, :ordered_term), do: :sext.decode(data)
end
