require "rocksdb"

module Store
  module Writer
    abstract def put(key : Bytes, value : Bytes)
    def put(key : String, value : String)
      put(key.to_slice, value.to_slice)
    end
  end

  module Reader
    abstract def get(key : Bytes)
    def get(key : String) : String
      String.new get(key.to_slice)
    end
  end

  class PrefixIter(T)
    include Iterator(Tuple(T, T))
    getter :curr, :value
    @iter : RocksDB::Iterator(T)
    @stopped = false
    @value : String?

    def initialize(@db : RocksDB::DB, @prefix : T)
      @iter = @db.new_iterator
      rewind
    end

    def next
      if @iter.valid? && @iter.key.starts_with?(@prefix)
        k = @iter.key
        v = @iter.value
        @iter.next
        return k, v
      end

      @stopped = true
      stop
    end

    def valid?
      !@stopped
    end

    def rewind
      @iter.seek(@prefix)
    end

    delegate key, value, close, to: @iter
  end

  class Store
    include Writer
    include Reader

    getter :set_operator

    def initialize(@path = "tmp/db")
      @db = RocksDB::DB.new(path)
    end

    def close
      @db.close
    end

    def put(key : Bytes, value : Bytes)
      @db.put(key, value)
    end

    def get(key : Bytes)
      @db.get(key)
    end

    def prefix(key_prefix : String)
      PrefixIter.new(@db, key_prefix)
    end

    def set_prefix(key_prefix : String)
      SetPrefixIter.new(@db, key_prefix)
    end

    def delete(key : String)
      @db.delete(key)
    end
  end

  class SetPrefixIter(T)
    include Iterator(T)

    def initialize(db : RocksDB::DB, @prefix : T)
      @iter = PrefixIter(T).new(db, @prefix)
    end

    def next
      result = @iter.next
      case result
      when {String, String}
        val = result.as({String, String})[0]
        return val.lchop(@prefix)
      else
        stop
      end
    end

    delegate :valid?, close, rewind, to: @iter
  end

  class UnionIterator(T)
    include Iterator(T)

    @started = false
    @val_1 : Iterator::Stop | String = ""
    @val_2 : Iterator::Stop | String = ""

    def initialize(@iter_1 : Iterator(T), @iter_2 : Iterator(T))
    end

    def valid?(iter_val)
      iter_val != Iterator.stop
    end

    def next
      unless @started
        @val_1 = @iter_1.next
        @val_2 = @iter_2.next
        @started = true
      end
      val_1 = @val_1
      val_2 = @val_2

      if valid?(@val_1) && valid?(@val_2)
        if val_1 == val_2
          @val_1 = @iter_1.next
          @val_2 = @iter_2.next
          return val_1
        end
        if @val_1.as(String) < @val_2.as(String)
          @val_1 = @iter_1.next
          return val_1
        end

        @val_2 = @iter_2.next
        return val_2
      end
      if valid?(val_1)
        @val_1 = @iter_1.next
        return val_1
      end
      if valid?(val_2)
        @val_2 = @iter_2.next
        return val_2
      end

      stop
    end
  end

  class IntersectionIterator(T)
    include Iterator(T)

    def initialize(@iter_1 : Iterator(T), @iter_2 : Iterator(T))
    end

    def valid?(iter_val)
      iter_val != Iterator.stop
    end

    def next
      val_1 = @iter_1.next
      val_2 = @iter_2.next
      while valid?(val_1) && valid?(val_2)
        if val_1 == val_2
          return val_1
        elsif val_1.as(T) < val_2.as(T)
          val_1 = @iter_1.next
        else
          val_2 = @iter_2.next
        end
      end

      stop
    end
  end

  class Set
    include Iterator(String)

    getter :prefix
    @prefix : String
    @iter : SetPrefixIter(String)? = nil

    def initialize(@store : Store, @key : String)
      @prefix = key_for("")
    end

    def key_for(value : String)
      "#{@key}⊃#{value}"
    end

    def add(*values)
      values.each { |v| @store.put(key_for(v), "") }
    end

    def remove(*values)
      values.each { |v| @store.delete(key_for(v)) }
    end

    def next
      @iter ||= @store.set_prefix(prefix)
      @iter.not_nil!.next
    end

    def rewind
      return self if @iter.nil?
      @iter.not_nil!.close
      @iter = nil
      self
    end

    def all
      rewind
      to_a
    end

    def union(other)
      UnionIterator.new(@store.set_prefix(@prefix), @store.set_prefix(other.prefix))
    end

    def intersection(other)
      IntersectionIterator.new(@store.set_prefix(@prefix), @store.set_prefix(other.prefix))
    end
  end
end
