module WAZ
  module Storage
    module VERSION #:nodoc:
      MAJOR    = '1'
      MINOR    = '0'
      TINY     = '3' 
    end
    
    Version = [VERSION::MAJOR, VERSION::MINOR, VERSION::TINY].compact * '.'
  end
end