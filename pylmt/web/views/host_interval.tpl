%#template to generate a page displaying the pyLMT results for a
%#given host and interval
<p>pyLMT - A web portal into the LMT databases at NERSC</p>
<table border="1">
  <tr>
    <td>
      host = {{host}}
    </td>
    <td>
      from {{begin}}
    </td>
    <td>
      to {{end}}
    </td>
  </tr>
  <tr>
    <td>

    </td>
    <td>
      scratch
    </td>
    <td>
      scratch2
    </td>
  </tr>
  <tr>
    <td>
      Bulk I/O
    </td>
    <td>
      <img src='{{bulk_scratch}}' alt='scratch bulk I/O with CPU utilization' height='300' width='400'/>
    </td>
    <td>
      <img src='{{bulk_scratch2}}' alt='scratch2 bulk I/O with CPU utilization' height='300' width='400'/>
    </td>
  </tr>
  <tr>
    <td>
      Metadata
    </td>
    <td>
      <img src='{{metadata_scratch}}' alt='scratch metadata with CPU utilization' height='300' width='400'/>
    </td>
    <td>
      <img src='{{metadata_scratch2}}' alt='scratch2 metadata with CPU utilization' height='300' width='400'/>
    </td>
  </tr>
</table>