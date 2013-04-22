%#template to generate a page displaying the pyLMT arguments when something has gone wrong
<p>pyLMT - The 'data' value did not seem to work right</p>
<table border="1">
  <tr>
    <td>
      host = {{host}}
    </td>
    <td>
      fs = {{fs}}
    </td>
  </tr>
  <tr>
    <td>
      begin = {{begin}}
    </td>
    <td>
      end = {{end}}
    </td>
  </tr>
  <tr>
    <td colspan='2'>
      data = {{data}}
    </td>
  </tr>
</table>