# Marko Kolarek's Personal Website

This repository contains the source code for my personal website and blog, accessible at [markokolarek.com](https://markokolarek.com).

## Overview

This website is built using [Hugo](https://gohugo.io/), a fast and modern static site generator. The site features a custom theme located in the `themes/blog-theme` directory.

## Features

- Personal introduction and about page
- Blog posts organized by categories and tags
- Resume/CV download
- Contact information
- Responsive design

## Getting Started

### Prerequisites

This project uses [Nix](https://nixos.org/) for dependency management. Ensure you have Nix installed on your system.

### Development

1. Clone this repository:
   ```
   git clone https://github.com/mkolarek/mkolarek.github.io.git
   cd mkolarek.github.io
   ```

2. Enter the Nix shell to load the required dependencies:
   ```
   nix-shell
   ```

3. Start the Hugo development server:
   ```
   hugo server -D
   ```

4. Open your browser and navigate to `http://localhost:1313` to see the site.

### Building for Production

To build the site for production:

```
hugo
```

This will generate the static site in the `public` directory.

## Project Structure

- `content/`: Contains all the website content in Markdown format
- `static/`: Contains static assets like images and PDFs
- `themes/blog-theme/`: Custom theme for the website
- `public/`: Generated site (not committed to the repository)
- `assets/`: Contains processed assets like CSS and JavaScript
- `shell.nix`: Nix configuration for development environment

## Deployment

This website is deployed using GitHub Pages. The static files in the `public` directory are served directly from the repository.

## Contact

- Website: [markokolarek.com](https://markokolarek.com)
- Email: marko@markokolarek.com
- LinkedIn: [mkolarek](https://linkedin.com/in/mkolarek)
- GitHub: [mkolarek](https://github.com/mkolarek)

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

The Hugo static site generator used by this project is also licensed under the [Apache License 2.0](https://github.com/gohugoio/hugo/blob/master/LICENSE).
