module NesterovFireIndex
  module_function

  def compute(apcp:, tmp:, dpt:)
    raise 'NesterovFireIndex.compute: size of data arrays must be equal' if (apcp.size != tmp.size) || (apcp.size != dpt.size)
    
    index_previous_day = 0

    indices = []

    (0...apcp.size).step(4) do |i|
      
      k = fire_index_of_day(index_previous_day, tmp[i, 4], dpt[i, 4])

      indices << k

      # set nesterov accumulator to zero
      apcp_sum_of_day = apcp[i, 4].sum

      if apcp_sum_of_day > 2.5 
        index_previous_day = 0 # set acc to zero
      else
        index_previous_day += k # continue to accumulate
      end
    end

    indices
  end

  def fire_index_of_day(index_previous_day, tmp, dpt)
    raise "fire_index_of_day error: lengths of tmp and dpt arrays must be equal" if tmp.length != dpt.length

    fire_indices = tmp.zip(dpt).map do |t, d| 
      fire_index(index_previous_day, t, d)
    end

    fire_indices.max
  end

  def fire_index(prev_value, tmp, dpt)
    tmp = tmp - 273.0
    dpt = dpt - 273.0
    index = prev_value + tmp * (tmp - dpt)
    if index < 0 || tmp < 0 then 
      0
    else
      index
    end
  end
end