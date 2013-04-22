%#template to generate a page displaying the pyLMT results
<p>pyLMT - A web portal into the LMT databases at NERSC</p>
<table border="1">
  <tr>
    <td>
      {{host}}
    </td>
    <td>
      {{filesys}}
    </td>
  </tr>
  <tr>
    <td>
      {{begin}}
    </td>
    <td>
      {{end}}
    </td>
  </tr>
  <tr>
    <td colspan='2'>
      data = {{data}}
    </td>
  </tr>
</table>