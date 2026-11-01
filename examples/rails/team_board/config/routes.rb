Rails.application.routes.draw do
  get "up" => "rails/health#show", as: :rails_health_check
  resource :session, only: %i[new create destroy]
  resources :projects, only: %i[index show] do
    resources :issues, only: %i[new create show update] do
      resources :comments, only: :create
    end
    resources :reports, only: %i[create show]
  end
  root "projects#index"
end
