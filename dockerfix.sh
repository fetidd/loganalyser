wincred_version=$(curl -fsSL -o /dev/null -w "%{url_effective}" https://github.com/docker/docker-credential-helpers/releases/latest | xargs basename)

# Downloads and extracts the .exe
sudo curl -fL -o /usr/local/bin/docker-credential-wincred.exe "https://github.com/docker/docker-credential-helpers/releases/download/${wincred_version}/docker-credential-wincred-${wincred_version}.windows-amd64.exe"

# Assigns execution permission to it
sudo chmod +x /usr/local/bin/docker-credential-wincred.exe
