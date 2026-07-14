function Resolve-BenchmarkInputFile {
  param(
    [Parameter(Mandatory = $true)]
    [string] $Root,

    [string] $File
  )

  if (-not $File) {
    return $null
  }

  $HostPath = if ([System.IO.Path]::IsPathRooted($File)) {
    $File
  } else {
    Join-Path $Root $File
  }

  if (-not (Test-Path -LiteralPath $HostPath)) {
    throw "Input file not found: $HostPath"
  }

  $ResolvedRoot = (Resolve-Path -LiteralPath $Root).Path
  $ResolvedFile = (Resolve-Path -LiteralPath $HostPath).Path
  if (-not $ResolvedFile.StartsWith($ResolvedRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Input file must be inside benchmark directory because Docker mounts only this directory: $ResolvedRoot"
  }

  $Relative = $ResolvedFile.Substring($ResolvedRoot.Length).TrimStart("\", "/")
  $ContainerRelative = $Relative.Replace("\", "/")

  return [pscustomobject]@{
    Host = $ResolvedFile
    ContainerRelative = $ContainerRelative
    ContainerAbsolute = "/benchmark/$ContainerRelative"
  }
}
